/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.common.protopath;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ProtobufPathParser {
  private static final Pattern REPEATED_FIELD_PATTERN = Pattern.compile("(\\w+)\\[(\\d+)]");
  private static final Pattern PLAIN_FIELD_PATTERN = Pattern.compile("(\\w+)");

  /**
   * Parses a (limited) {@code ProtocolBuffer}'s path expression.
   *
   * <p>A {@code ProtocolBuffer}'s path expression applied to a root {@link
   * com.google.protobuf.Message} and can be one of the following:
   *
   * <ul>
   *   <li>Field selector - donated by a {@code .field} expression.
   *   <li>A repeated filed index - donated by {@code .field[index]} expression.
   * </ul>
   *
   * <p>Each path expression starts with a {@code $} symbol to donate the root message. For example,
   * with this message type:
   *
   * <pre>{@code
   * message Foo {
   *     string baz = 1;
   * }
   * }</pre>
   *
   * The following expression can select the field {@code baz}: {@code $.baz}.
   *
   * @param protobufPath an {@code ProtocolBuffer}'s path expression.
   * @return an ordered list of path fragments.
   */
  static List<PathFragment> parse(String protobufPath) {
    validatePrefix(protobufPath);
    String[] tokens = protobufPath.substring(2).split("\\.");

    List<PathFragment> pathFragments = new ArrayList<>();

    for (String token : tokens) {
      Matcher repeatedFieldMatcher = REPEATED_FIELD_PATTERN.matcher(token);
      if (repeatedFieldMatcher.matches()) {
        pathFragments.add(parseRepeatedField(protobufPath, repeatedFieldMatcher));
        continue;
      }
      Matcher plainFieldMatcher = PLAIN_FIELD_PATTERN.matcher(token);
      if (plainFieldMatcher.matches()) {
        pathFragments.add(parsePlainField(plainFieldMatcher));
        continue;
      }
      throw new IllegalArgumentException("Parse error in " + protobufPath + " at " + token);
    }
    return pathFragments;
  }

  private static PathFragment parsePlainField(Matcher plainFieldMatcher) {
    String fieldName = plainFieldMatcher.group(1);
    return new PathFragment(fieldName);
  }

  private static PathFragment parseRepeatedField(
      String protobufPath, Matcher repeatedFieldMatcher) {
    String fieldName = repeatedFieldMatcher.group(1);
    String indexString = repeatedFieldMatcher.group(2);
    int index = Integer.parseInt(indexString);
    if (index < 0) {
      throw new IllegalArgumentException(
          "Parse error in "
              + protobufPath
              + " at a repeated field "
              + fieldName
              + " index is negative "
              + index);
    }
    return new PathFragment(fieldName, index);
  }

  private static void validatePrefix(String protobufPath) {
    if (protobufPath.length() < 2) {
      throw new IllegalArgumentException("Path is empty");
    }
    if (protobufPath.charAt(0) != '$') {
      throw new IllegalArgumentException("Path must start with a $ sign");
    }
    if (protobufPath.charAt(1) != '.') {
      throw new IllegalArgumentException("A field access must start with a .");
    }
  }
}
