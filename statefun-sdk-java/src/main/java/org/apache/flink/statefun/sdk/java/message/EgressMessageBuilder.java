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
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

/**
 * A Custom {@link EgressMessage} builder.
 *
 * <p>To use {code Kafka} specific builder please use {@link
 * org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage}. To use {code Kinesis} specific egress
 * please use {@link org.apache.flink.statefun.sdk.egress.generated.KinesisEgress}.
 *
 * <p>Use this builder if you need to send message to a custom egress defined via the embedded SDK.
 */
public final class EgressMessageBuilder {
  private final TypeName target;
  private final TypedValue.Builder builder;

  public static EgressMessageBuilder forEgress(TypeName targetEgress) {
    return new EgressMessageBuilder(targetEgress);
  }

  private EgressMessageBuilder(TypeName target) {
    this.target = Objects.requireNonNull(target);
    this.builder = TypedValue.newBuilder();
  }

  public EgressMessageBuilder withValue(long value) {
    return withCustomType(Types.longType(), value);
  }

  public EgressMessageBuilder withValue(int value) {
    return withCustomType(Types.integerType(), value);
  }

  public EgressMessageBuilder withValue(boolean value) {
    return withCustomType(Types.booleanType(), value);
  }

  public EgressMessageBuilder withValue(String value) {
    return withCustomType(Types.stringType(), value);
  }

  public EgressMessageBuilder withValue(float value) {
    return withCustomType(Types.floatType(), value);
  }

  public EgressMessageBuilder withValue(double value) {
    return withCustomType(Types.doubleType(), value);
  }

  public <T> EgressMessageBuilder withCustomType(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    TypeSerializer<T> typeSerializer = customType.typeSerializer();
    builder.setTypenameBytes(ApiExtension.typeNameByteString(customType.typeName()));
    Slice serialized = typeSerializer.serialize(element);
    ByteString serializedByteString = SliceProtobufUtil.asByteString(serialized);
    builder.setValue(serializedByteString);
    builder.setHasValue(true);
    return this;
  }

  public EgressMessage build() {
    return new EgressMessageWrapper(target, builder.build());
  }
}
