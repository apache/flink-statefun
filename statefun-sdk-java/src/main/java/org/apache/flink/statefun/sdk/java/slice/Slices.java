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

import com.google.protobuf.ByteString;
import com.google.protobuf.MoreByteStrings;
import java.io.InputStream;
import java.nio.ByteBuffer;

public final class Slices {
  private Slices() {}

  public static Slice wrap(ByteBuffer buffer) {
    return wrap(MoreByteStrings.wrap(buffer));
  }

  public static Slice wrap(byte[] bytes) {
    return wrap(MoreByteStrings.wrap(bytes));
  }

  private static Slice wrap(ByteString bytes) {
    return new ByteStringSlice(bytes);
  }

  public static Slice wrap(byte[] bytes, int offset, int len) {
    return wrap(MoreByteStrings.wrap(bytes, offset, len));
  }

  public static Slice copyOf(byte[] bytes) {
    return wrap(ByteString.copyFrom(bytes));
  }

  public static Slice copyOf(byte[] bytes, int offset, int len) {
    return wrap(ByteString.copyFrom(bytes, offset, len));
  }

  public static Slice copyOf(InputStream inputStream, int expectedStreamSize) {
    SliceOutput out = SliceOutput.sliceOutput(expectedStreamSize);
    out.writeFully(inputStream);
    return out.view();
  }

  public static Slice copyFromUtf8(String input) {
    return wrap(ByteString.copyFromUtf8(input));
  }
}
