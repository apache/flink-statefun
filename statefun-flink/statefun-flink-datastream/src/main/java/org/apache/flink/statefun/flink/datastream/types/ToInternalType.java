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

package org.apache.flink.statefun.flink.datastream.types;

import com.google.protobuf.ByteString;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.types.Type;
import org.apache.flink.statefun.sdk.types.TypeSerializer;

/** Converts each message to a {@link RoutableMessage}. */
@Internal
public class ToInternalType<T> extends RichMapFunction<T, RoutableMessage> {

  private final Router<T> router;

  private final Type<T> type;

  private transient TypeSerializer<T> typeSerializer;

  private transient ByteString typeNameByteString;

  public ToInternalType(Type<T> type, Router<T> router) {
    this.type = type;
    this.router = router;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    typeSerializer = type.typeSerializer();
    typeNameByteString = ByteString.copyFromUtf8(type.typeName().canonicalTypenameString());
  }

  @Override
  public RoutableMessage map(T message) {
    TypedValue typedValue =
        TypedValue.newBuilder()
            .setTypenameBytes(typeNameByteString)
            .setHasValue(true)
            .setValue(SliceProtobufUtil.asByteString(typeSerializer.serialize(message)))
            .build();

    return RoutableMessageBuilder.builder()
        .withTargetAddress(router.route(message))
        .withMessageBody(typedValue)
        .build();
  }
}
