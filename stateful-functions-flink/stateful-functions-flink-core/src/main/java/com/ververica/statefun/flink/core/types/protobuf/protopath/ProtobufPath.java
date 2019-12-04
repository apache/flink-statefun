/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica.statefun.flink.core.types.protobuf.protopath;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.function.Function;

public final class ProtobufPath {

  /**
   * Compile a (limited) {@code ProtocolBuffer}'s path expression.
   *
   * <p>A {@code ProtocolBuffer}'s path expression applied to a {@link com.google.protobuf.Message}
   * and can be one of the following:
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
   * @param pathString an {@code ProtocolBuffer}'s path expression.
   * @return an ordered list of path fragments.
   */
  public static Function<DynamicMessage, ?> protobufPath(
      Descriptors.Descriptor messageDescriptor, String pathString) {
    List<PathFragment> fields = ProtobufPathParser.parse(pathString);
    List<PathFragmentDescriptor> pathFragments =
        ProtobufPathCompiler.compile(messageDescriptor, fields);
    return new ProtobufDynamicMessageLens(pathFragments);
  }
}
