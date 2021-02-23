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

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class MessageBuilder {
  private final TypedValue.Builder builder;
  private Address targetAddress;

  private MessageBuilder(TypeName functionType, String id) {
    this(functionType, id, TypedValue.newBuilder());
  }

  private MessageBuilder(TypeName functionType, String id, TypedValue.Builder builder) {
    this.targetAddress = new Address(functionType, id);
    this.builder = Objects.requireNonNull(builder);
  }

  public static MessageBuilder forAddress(TypeName functionType, String id) {
    return new MessageBuilder(functionType, id);
  }

  public static MessageBuilder forAddress(Address address) {
    Objects.requireNonNull(address);
    return new MessageBuilder(address.type(), address.id());
  }

  public static MessageBuilder fromMessage(Message message) {
    Address targetAddress = message.targetAddress();
    TypedValue.Builder builder = typedValueBuilder(message);
    return new MessageBuilder(targetAddress.type(), targetAddress.id(), builder);
  }

  public MessageBuilder withValue(long value) {
    return withCustomType(Types.longType(), value);
  }

  public MessageBuilder withValue(int value) {
    return withCustomType(Types.integerType(), value);
  }

  public MessageBuilder withValue(boolean value) {
    return withCustomType(Types.booleanType(), value);
  }

  public MessageBuilder withValue(String value) {
    return withCustomType(Types.stringType(), value);
  }

  public MessageBuilder withValue(float value) {
    return withCustomType(Types.floatType(), value);
  }

  public MessageBuilder withValue(double value) {
    return withCustomType(Types.doubleType(), value);
  }

  public MessageBuilder withTargetAddress(Address targetAddress) {
    this.targetAddress = Objects.requireNonNull(targetAddress);
    return this;
  }

  public MessageBuilder withTargetAddress(TypeName typeName, String id) {
    return withTargetAddress(new Address(typeName, id));
  }

  public <T> MessageBuilder withCustomType(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    TypeSerializer<T> typeSerializer = customType.typeSerializer();
    builder.setTypenameBytes(ApiExtension.typeNameByteString(customType.typeName()));
    Slice serialized = typeSerializer.serialize(element);
    ByteString serializedByteString = SliceProtobufUtil.asByteString(serialized);
    builder.setValue(serializedByteString);
    return this;
  }

  public Message build() {
    return new MessageWrapper(targetAddress, builder.build());
  }

  private static TypedValue.Builder typedValueBuilder(Message message) {
    ByteString typenameBytes = ApiExtension.typeNameByteString(message.valueTypeName());
    ByteString valueBytes = SliceProtobufUtil.asByteString(message.rawValue());
    return TypedValue.newBuilder().setTypenameBytes(typenameBytes).setValue(valueBytes);
  }
}
