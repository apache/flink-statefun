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
package org.apache.flink.statefun.flink.core.nettyclient;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.util.Preconditions;

final class NettyProtobuf {

  public static <M extends Message> ByteBuf serializeProtobuf(
      IntFunction<ByteBuf> allocator, M message) {
    final int requiredSize = message.getSerializedSize();
    final ByteBuf buf = allocator.apply(requiredSize);
    try {
      if (buf.nioBufferCount() == 1) {
        zeroCopySerialize(message, requiredSize, buf);
      } else {
        serializeOutputStream(message, buf);
      }
      return buf;
    } catch (IOException e) {
      buf.release();
      throw new UncheckedIOException(e);
    }
  }

  public static <M extends Message> M deserializeProtobuf(ByteBuf buf, Parser<M> parser) {
    try {
      if (buf.nioBufferCount() == 1) {
        return zeroCopyDeserialize(buf, parser);
      } else {
        return deserializeInputStream(buf, parser);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <M extends Message> void zeroCopySerialize(M message, int len, ByteBuf buf)
      throws IOException {
    Preconditions.checkState(len <= buf.writableBytes());
    final int originalWriterIndex = buf.writerIndex();
    ByteBuffer nioBuf = buf.nioBuffer(originalWriterIndex, len);
    CodedOutputStream out = CodedOutputStream.newInstance(nioBuf);
    message.writeTo(out);
    out.flush();
    buf.writerIndex(originalWriterIndex + len);
  }

  private static <M extends Message> void serializeOutputStream(M message, ByteBuf buf)
      throws IOException {
    message.writeTo(new ByteBufOutputStream(buf));
  }

  private static <M extends Message> M zeroCopyDeserialize(ByteBuf buf, Parser<M> parser)
      throws InvalidProtocolBufferException {
    final int messageLength = buf.readableBytes();
    final int originalReaderIndex = buf.readerIndex();
    ByteBuffer nioBuffer = buf.nioBuffer(originalReaderIndex, messageLength);
    CodedInputStream in = CodedInputStream.newInstance(nioBuffer);
    M message = parser.parseFrom(in);
    buf.readerIndex(originalReaderIndex + messageLength);
    return message;
  }

  private static <M extends Message> M deserializeInputStream(ByteBuf buf, Parser<M> parser)
      throws InvalidProtocolBufferException {
    return parser.parseFrom(new ByteBufInputStream(buf));
  }
}
