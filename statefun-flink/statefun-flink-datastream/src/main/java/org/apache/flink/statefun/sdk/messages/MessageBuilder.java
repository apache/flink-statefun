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

package org.apache.flink.statefun.sdk.messages;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.SdkMessage;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.slice.Slice;
import org.apache.flink.statefun.sdk.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.types.Type;
import org.apache.flink.statefun.sdk.types.TypeSerializer;
import org.apache.flink.statefun.sdk.types.Types;

/**
 * {@link MessageBuilder} creates messages that can be sent to stateful functions, routed to the
 * target address. The {@code #withValue} methods will automatically convert elements into the
 * StateFun type system. These messages are then automatically consumable by any remote langauge SDK
 * - Python, Java, etc.
 *
 * <p>{@code MessageBuilder.forAddress(new FunctionType("example", "python-function"),
 * "id").withValue("hello")}
 */
public final class MessageBuilder {
  private final Address targetAddress;

  private MessageBuilder(FunctionType functionType, String id) {
    this.targetAddress = new Address(functionType, id);
  }

  public static MessageBuilder forAddress(FunctionType functionType, String id) {
    return new MessageBuilder(functionType, id);
  }

  public static MessageBuilder forAddress(TypeName functionType, String id) {
    return new MessageBuilder(new FunctionType(functionType.namespace(), functionType.name()), id);
  }

  public static MessageBuilder forAddress(Address address) {
    Objects.requireNonNull(address);
    return new MessageBuilder(address.type(), address.id());
  }

  public RoutableMessage withValue(boolean value) {
    return withCustomType(Types.booleanType(), value);
  }

  public RoutableMessage withValue(int value) {
    return withCustomType(Types.integerType(), value);
  }

  public RoutableMessage withValue(long value) {
    return withCustomType(Types.longType(), value);
  }

  public RoutableMessage withValue(String value) {
    return withCustomType(Types.stringType(), value);
  }

  public RoutableMessage withValue(float value) {
    return withCustomType(Types.floatType(), value);
  }

  public RoutableMessage withValue(double value) {
    return withCustomType(Types.doubleType(), value);
  }

  public <T> RoutableMessage withCustomType(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    TypeSerializer<T> typeSerializer = customType.typeSerializer();
    TypedValue.Builder builder = TypedValue.newBuilder();
    builder.setTypename(customType.typeName().canonicalTypenameString());
    Slice serialized = typeSerializer.serialize(element);
    ByteString serializedByteString = SliceProtobufUtil.asByteString(serialized);
    builder.setValue(serializedByteString);
    builder.setHasValue(true);
    return new SdkMessage(null, targetAddress, builder.build());
  }

  /**
   * A special utility method that does not convert the target element into the stateful functions
   * type system. These objects are only consumable by embedded {@link
   * org.apache.flink.statefun.sdk.StatefulFunction} instances.
   */
  public RoutableMessage withJavaObject(Object element) {
    return new SdkMessage(null, targetAddress, element);
  }
}
