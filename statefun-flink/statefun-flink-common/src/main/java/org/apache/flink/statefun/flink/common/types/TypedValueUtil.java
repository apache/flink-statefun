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

package org.apache.flink.statefun.flink.common.types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class TypedValueUtil {

  private TypedValueUtil() {}

  public static boolean isProtobufTypeOf(
      TypedValue typedValue, Descriptors.Descriptor messageDescriptor) {
    return typedValue.getTypename().equals(protobufTypeUrl(messageDescriptor));
  }

  public static TypedValue packProtobufMessage(Message protobufMessage) {
    return TypedValue.newBuilder()
        .setTypename(protobufTypeUrl(protobufMessage.getDescriptorForType()))
        .setValue(protobufMessage.toByteString())
        .build();
  }

  public static <PB extends Message> PB unpackProtobufMessage(
      TypedValue typedValue, Parser<PB> protobufMessageParser) {
    try {
      return protobufMessageParser.parseFrom(typedValue.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String protobufTypeUrl(Descriptors.Descriptor messageDescriptor) {
    return "type.googleapis.com/" + messageDescriptor.getFullName();
  }
}
