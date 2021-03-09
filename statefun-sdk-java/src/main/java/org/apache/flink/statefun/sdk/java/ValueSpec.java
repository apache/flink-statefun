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

package org.apache.flink.statefun.sdk.java;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

public final class ValueSpec<T> {

  /**
   * Creates an {@link Untyped} spec with the given name. To complete the creation of a {@link
   * ValueSpec}, please specify the {@link Type} of the value. For example:
   *
   * <pre>{@code
   * final ValueSpec<Integer> intValue = ValueSpec.named("my_int_state").withIntType();
   * }</pre>
   *
   * <p>The specified name string must be a valid identifier conforming to the following rules:
   *
   * <ul>
   *   <li>First character must be an alphabet letter [a-z] / [A-Z], or an underscore '_'.
   *   <li>Remaining characters can be an alphabet letter [a-z] / [A-Z], a digit [0-9], or an
   *       underscore '_'.
   *   <li>Must not contain any spaces.
   * </ul>
   *
   * @param name name for the value. Please see the method Javadocs for format rules.
   * @return an {@link Untyped} spec. Specify the {@link Type} of the value to instantiate a {@link
   *     ValueSpec}.
   */
  public static Untyped named(String name) {
    validateStateName(name);
    return new Untyped(name);
  }

  private final String name;
  private final Expiration expiration;
  private final Type<T> type;
  private final ByteString nameByteString;

  private ValueSpec(Untyped untyped, Type<T> type) {
    Objects.requireNonNull(untyped);
    Objects.requireNonNull(type);
    this.name = untyped.stateName;
    this.expiration = untyped.expiration;
    this.type = Objects.requireNonNull(type);
    this.nameByteString = ByteString.copyFromUtf8(untyped.stateName);
  }

  public String name() {
    return name;
  }

  public Expiration expiration() {
    return expiration;
  }

  public TypeName typeName() {
    return type.typeName();
  }

  public Type<T> type() {
    return type;
  }

  ByteString nameByteString() {
    return nameByteString;
  }

  public static final class Untyped {
    private final String stateName;
    private Expiration expiration = Expiration.none();

    public Untyped(String name) {
      this.stateName = Objects.requireNonNull(name);
    }

    public Untyped thatExpireAfterWrite(Duration duration) {
      this.expiration = Expiration.expireAfterWriting(duration);
      return this;
    }

    public Untyped thatExpiresAfterCall(Duration duration) {
      this.expiration = Expiration.expireAfterCall(duration);
      return this;
    }

    public ValueSpec<Integer> withIntType() {
      return withCustomType(Types.integerType());
    }

    public ValueSpec<Long> withLongType() {
      return withCustomType(Types.longType());
    }

    public ValueSpec<Float> withFloatType() {
      return withCustomType(Types.floatType());
    }

    public ValueSpec<Double> withDoubleType() {
      return withCustomType(Types.doubleType());
    }

    public ValueSpec<String> withUtf8StringType() {
      return withCustomType(Types.stringType());
    }

    public ValueSpec<Boolean> withBooleanType() {
      return new ValueSpec<>(this, Types.booleanType());
    }

    public <T> ValueSpec<T> withCustomType(Type<T> type) {
      Objects.requireNonNull(type);
      return new ValueSpec<>(this, type);
    }
  }

  private static void validateStateName(String stateName) {
    Objects.requireNonNull(stateName);
    final char[] chars = stateName.toCharArray();

    if (!isValidStateNameStart(chars[0])) {
      throw new IllegalArgumentException(
          "Invalid state name: "
              + stateName
              + ". State names can only start with alphabet letters [a-z] / [A-Z], or an underscore ('_').");
    }

    for (int i = 1; i < chars.length; i++) {
      if (!isValidStateNamePart(chars[i])) {
        throw new IllegalArgumentException(
            "Invalid state name: "
                + stateName
                + ". State names can only contain alphabet letters [a-z] / [A-Z], digits [0-9], or underscores ('_').");
      }
    }
  }

  private static boolean isValidStateNameStart(char character) {
    return Character.isLetter(character) || character == '_';
  }

  private static boolean isValidStateNamePart(char character) {
    return Character.isLetter(character) || Character.isDigit(character) || character == '_';
  }
}
