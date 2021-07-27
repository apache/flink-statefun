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

package org.apache.flink.statefun.flink.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.TypedValue;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.types.Type;
import org.apache.flink.statefun.sdk.types.TypeSerializer;
import org.apache.flink.statefun.sdk.types.Types;

/**
 * Identifies a generic egress.
 *
 * @param <T> The Java object of the output records.
 */
public class GenericEgress<T> {
  private final EgressIdentifier<org.apache.flink.statefun.sdk.reqreply.generated.TypedValue> id;

  private final Type<T> type;

  private final TypeInformation<T> typeInfo;

  private GenericEgress(TypeName typeName, Type<T> type, TypeInformation<T> typeInfo) {
    this.id =
        new EgressIdentifier<>(
            typeName.name(),
            typeName.name(),
            org.apache.flink.statefun.sdk.reqreply.generated.TypedValue.class);
    this.type = type;
    this.typeInfo = typeInfo;
  }

  @Override
  public String toString() {
    return "GenericEgress{"
        + "typeName="
        + id.namespace()
        + "/"
        + id.name()
        + ", type="
        + type
        + '}';
  }

  @Internal
  EgressIdentifier<org.apache.flink.statefun.sdk.reqreply.generated.TypedValue> getId() {
    return this.id;
  }

  @Internal
  public Type<T> getType() {
    return this.type;
  }

  @Internal
  public TypeInformation<T> getTypeInfo() {
    return this.typeInfo;
  }

  /**
   * Creates an {@link Untyped} generic egress with the given name. To complete the creation of a
   * {@link GenericEgress}, please specify the {@link Type} of the value. For example:
   *
   * <pre>{@code
   * final GenericEgress<Integer> intEgress =
   *    GenericEgress.named(TypeName.parseFrom("example/egress")).withIntType();
   * }</pre>
   *
   * @param typeName unique name for the GenericEgress.
   * @return an {@link Untyped} spec. Specify the {@link Type} of the value to instantiate a {@link
   *     GenericEgress}.
   */
  public static Untyped named(TypeName typeName) {
    return new Untyped(typeName);
  }

  public static class Untyped {
    private final TypeName typeName;

    private Untyped(TypeName typeName) {
      this.typeName = typeName;
    }

    /** @return A {@link GenericEgress} for consuming integers. */
    public GenericEgress<Integer> withIntType() {
      return withCustomType(Types.integerType(), BasicTypeInfo.INT_TYPE_INFO);
    }

    /** @return A {@link GenericEgress} for consuming longs. */
    public GenericEgress<Long> withLongType() {
      return withCustomType(Types.longType(), BasicTypeInfo.LONG_TYPE_INFO);
    }

    /** @return A {@link GenericEgress} for consuming floats. */
    public GenericEgress<Float> withFloatType() {
      return withCustomType(Types.floatType(), BasicTypeInfo.FLOAT_TYPE_INFO);
    }

    /** @return A {@link GenericEgress} for consuming doubles. */
    public GenericEgress<Double> withDoubleType() {
      return withCustomType(Types.doubleType(), BasicTypeInfo.DOUBLE_TYPE_INFO);
    }

    /** @return A {@link GenericEgress} for consuming strings. */
    public GenericEgress<String> withUtf8StringType() {
      return withCustomType(Types.stringType(), BasicTypeInfo.STRING_TYPE_INFO);
    }

    /** @return A {@link GenericEgress} for consuming booleans. */
    public GenericEgress<Boolean> withBooleanType() {
      return withCustomType(Types.booleanType(), BasicTypeInfo.BOOLEAN_TYPE_INFO);
    }

    /**
     * @return A {@link GenericEgress} that does not decode the output messages. It instead returns
     *     each {@link TypedValue} containing the {@link TypeName} and serialized bytes.
     */
    public GenericEgress<TypedValue> withTypedValue() {
      return withCustomType(new TypedValueType(), TypeInformation.of(TypedValue.class));
    }

    /**
     * A {@link GenericEgress} for custom types.
     *
     * @param type The decoding type.
     * @param clazz The resulting Java class.
     * @param <T> The resulting Java type.
     * @return A custom generic egress.
     */
    public <T> GenericEgress<T> withCustomType(Type<T> type, Class<T> clazz) {
      return new GenericEgress<>(typeName, type, TypeInformation.of(clazz));
    }

    /**
     * A {@link GenericEgress} for custom types.
     *
     * @param type The decoding type.
     * @param typeInfo The resulting Java {@link TypeInformation}.
     * @param <T> The resulting Java type.
     * @return A custom generic egress.
     */
    public <T> GenericEgress<T> withCustomType(Type<T> type, TypeInformation<T> typeInfo) {
      return new GenericEgress<>(typeName, type, typeInfo);
    }
  }

  /** A shim class that is never used. */
  private static class TypedValueType implements Type<TypedValue> {

    @Override
    public TypeName typeName() {
      throw new RuntimeException();
    }

    @Override
    public TypeSerializer<TypedValue> typeSerializer() {
      throw new RuntimeException();
    }
  }
}
