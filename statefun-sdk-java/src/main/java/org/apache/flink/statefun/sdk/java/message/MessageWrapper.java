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
package org.apache.flink.statefun.sdk.java.message;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@Internal
public final class MessageWrapper implements Message {
  private final TypedValue typedValue;
  private final Address targetAddress;

  public MessageWrapper(Address targetAddress, TypedValue typedValue) {
    this.targetAddress = Objects.requireNonNull(targetAddress);

    if (!typedValue.getHasValue()) {
      throw new IllegalStateException("Unset empty Messages are prohibited.");
    }
    this.typedValue = Objects.requireNonNull(typedValue);
  }

  @Override
  public Address targetAddress() {
    return targetAddress;
  }

  @Override
  public boolean isLong() {
    return is(Types.longType());
  }

  @Override
  public long asLong() {
    return as(Types.longType());
  }

  @Override
  public boolean isUtf8String() {
    return is(Types.stringType());
  }

  @Override
  public String asUtf8String() {
    return as(Types.stringType());
  }

  @Override
  public boolean isInt() {
    return is(Types.integerType());
  }

  @Override
  public int asInt() {
    return as(Types.integerType());
  }

  @Override
  public boolean isBoolean() {
    return is(Types.booleanType());
  }

  @Override
  public boolean asBoolean() {
    return as(Types.booleanType());
  }

  @Override
  public boolean isFloat() {
    return is(Types.floatType());
  }

  @Override
  public float asFloat() {
    return as(Types.floatType());
  }

  @Override
  public boolean isDouble() {
    return is(Types.doubleType());
  }

  @Override
  public double asDouble() {
    return as(Types.doubleType());
  }

  @Override
  public <T> boolean is(Type<T> type) {
    String thisTypeNameString = typedValue.getTypename();
    String thatTypeNameString = type.typeName().asTypeNameString();
    return thisTypeNameString.equals(thatTypeNameString);
  }

  @Override
  public <T> T as(Type<T> type) {
    TypeSerializer<T> typeSerializer = type.typeSerializer();
    Slice input = SliceProtobufUtil.asSlice(typedValue.getValue());
    return typeSerializer.deserialize(input);
  }

  @Override
  public TypeName valueTypeName() {
    return TypeName.typeNameFromString(typedValue.getTypename());
  }

  @Override
  public Slice rawValue() {
    return SliceProtobufUtil.asSlice(typedValue.getValue());
  }

  public TypedValue typedValue() {
    return typedValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageWrapper that = (MessageWrapper) o;
    return Objects.equals(typedValue, that.typedValue)
        && Objects.equals(targetAddress, that.targetAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typedValue, targetAddress);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MessageWrapper{");
    sb.append("typedValue=").append(typedValue);
    sb.append(", targetAddress=").append(targetAddress);
    sb.append('}');
    return sb.toString();
  }
}
