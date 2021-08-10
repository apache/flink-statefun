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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.junit.After;
import org.junit.Test;

public class NettyProtobufTest {

  @After
  public void tearDown() {
    ALLOCATOR.close();
  }

  private final AutoReleasingAllocator ALLOCATOR = new AutoReleasingAllocator();

  @Test
  public void roundTrip() {
    char[] chars = new char[1024 * 1024];
    Arrays.fill(chars, 'x');
    String pad = new String(chars);

    for (int i = 0; i < 100; i++) {
      int size = ThreadLocalRandom.current().nextInt(1, pad.length());
      Address original =
          Address.newBuilder()
              .setNamespace("namespace")
              .setType("type")
              .setId(pad.substring(0, size))
              .build();

      Address actual = serdeRoundTrip(ALLOCATOR, original);

      assertThat(actual, is(original));
    }
  }

  @Test
  public void heapBufferRoundTrip() {
    char[] chars = new char[1024 * 1024];
    Arrays.fill(chars, 'x');
    String pad = new String(chars);

    IntFunction<ByteBuf> heapAllocator = ByteBufAllocator.DEFAULT::heapBuffer;

    for (int i = 0; i < 100; i++) {
      int size = ThreadLocalRandom.current().nextInt(1, pad.length());
      Address original =
          Address.newBuilder()
              .setNamespace("namespace")
              .setType("type")
              .setId(pad.substring(0, size))
              .build();

      Address actual = serdeRoundTrip(heapAllocator, original);
      assertThat(actual, is(original));
    }
  }

  private Address serdeRoundTrip(IntFunction<ByteBuf> allocator, Address original) {
    ByteBuf buf = NettyProtobuf.serializeProtobuf(allocator, original);
    Address got = NettyProtobuf.deserializeProtobuf(buf, Address.parser());
    buf.release();
    return got;
  }

  private static final class AutoReleasingAllocator implements IntFunction<ByteBuf>, AutoCloseable {
    private final ArrayDeque<ByteBuf> allocatedDuringATest = new ArrayDeque<>();

    @Override
    public ByteBuf apply(int value) {
      ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(value);
      allocatedDuringATest.addLast(buf);
      return buf;
    }

    @Override
    public void close() {
      for (ByteBuf buf : allocatedDuringATest) {
        int refCount = buf.refCnt();
        if (refCount > 0) {
          buf.release(refCount);
        }
      }
    }
  }
}
