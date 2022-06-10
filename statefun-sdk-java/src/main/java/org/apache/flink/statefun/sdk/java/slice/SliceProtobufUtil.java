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
package org.apache.flink.statefun.sdk.java.slice;

import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.Message;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.MoreByteStrings;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.Parser;

@Internal
public final class SliceProtobufUtil {
  private SliceProtobufUtil() {}

  public static <T> T parseFrom(Parser<T> parser, Slice slice)
      throws InvalidProtocolBufferException {
    if (slice instanceof ByteStringSlice) {
      ByteString byteString = ((ByteStringSlice) slice).byteString();
      return parser.parseFrom(byteString);
    }
    return parser.parseFrom(slice.asReadOnlyByteBuffer());
  }

  public static Slice toSlice(Message message) {
    return Slices.wrap(message.toByteArray());
  }

  public static ByteString asByteString(Slice slice) {
    if (slice instanceof ByteStringSlice) {
      ByteStringSlice byteStringSlice = (ByteStringSlice) slice;
      return byteStringSlice.byteString();
    }
    return MoreByteStrings.wrap(slice.asReadOnlyByteBuffer());
  }

  public static Slice asSlice(ByteString byteString) {
    return new ByteStringSlice(byteString);
  }
}
