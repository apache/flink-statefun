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
package org.apache.flink.statefun.sdk.slice;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

final class ByteStringSlice implements Slice {
  private final ByteString byteString;

  public ByteStringSlice(ByteString bytes) {
    this.byteString = Objects.requireNonNull(bytes);
  }

  public ByteString byteString() {
    return byteString;
  }

  @Override
  public ByteBuffer asReadOnlyByteBuffer() {
    return byteString.asReadOnlyByteBuffer();
  }

  @Override
  public int readableBytes() {
    return byteString.size();
  }

  @Override
  public void copyTo(byte[] target) {
    copyTo(target, 0);
  }

  @Override
  public void copyTo(byte[] target, int targetOffset) {
    byteString.copyTo(target, targetOffset);
  }

  @Override
  public void copyTo(OutputStream outputStream) {
    try {
      byteString.writeTo(outputStream);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public byte byteAt(int position) {
    return byteString.byteAt(position);
  }

  @Override
  public void copyTo(ByteBuffer buffer) {
    byteString.copyTo(buffer);
  }

  @Override
  public byte[] toByteArray() {
    return byteString.toByteArray();
  }
}
