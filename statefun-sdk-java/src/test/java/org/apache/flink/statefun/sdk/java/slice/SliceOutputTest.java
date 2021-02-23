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

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class SliceOutputTest {

  private static final byte[] TEST_BYTES = new byte[] {1, 2, 3, 4, 5};

  @Test
  public void usageExample() {
    SliceOutput output = SliceOutput.sliceOutput(32);

    output.write(TEST_BYTES);
    Slice slice = output.copyOf();

    assertArrayEquals(TEST_BYTES, slice.toByteArray());
  }

  @Test
  public void sliceShouldAutoGrow() {
    SliceOutput sliceOutput = SliceOutput.sliceOutput(0);

    sliceOutput.write(TEST_BYTES);
    Slice slice = sliceOutput.copyOf();
    byte[] got = new byte[slice.readableBytes()];
    slice.copyTo(got);

    assertArrayEquals(got, TEST_BYTES);
  }

  @Test
  public void writeStartingAtOffset() {
    SliceOutput output = SliceOutput.sliceOutput(32);

    output.write(TEST_BYTES);
    Slice slice = output.copyOf();

    byte[] slightlyBiggerBuf = new byte[1 + slice.readableBytes()];
    slice.copyTo(slightlyBiggerBuf, 1);

    byte[] got = deleteFirstByte(slightlyBiggerBuf);
    assertArrayEquals(TEST_BYTES, got);
  }

  @Test
  public void randomizedTest() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < 1_000_000; i++) {
      // create a random buffer of a random size.
      final int size = random.nextInt(0, 32);
      byte[] buf = randomBuffer(random, size);
      // write the buffer in random chunks
      SliceOutput sliceOutput = SliceOutput.sliceOutput(0);
      for (OffsetLenPair chunk : randomChunks(random, size)) {
        sliceOutput.write(buf, chunk.offset, chunk.len);
      }
      // verify
      Slice slice = sliceOutput.copyOf();
      assertArrayEquals(buf, slice.toByteArray());
    }
  }

  @Test
  public void copyFromInputStreamRandomizedTest() {
    byte[] expected = randomBuffer(ThreadLocalRandom.current(), 256);
    ByteArrayInputStream input = new ByteArrayInputStream(expected);

    Slice slice = Slices.copyOf(input, 0);

    byte[] actual = slice.toByteArray();

    assertArrayEquals(expected, actual);
  }

  @Test
  public void toOutputStreamTest() {
    byte[] expected = randomBuffer(ThreadLocalRandom.current(), 256);

    Slice slice = Slices.wrap(expected);

    ByteArrayOutputStream output = new ByteArrayOutputStream(expected.length);
    slice.copyTo(output);

    assertArrayEquals(expected, output.toByteArray());
  }

  private static byte[] deleteFirstByte(byte[] slightlyBiggerBuf) {
    return Arrays.copyOfRange(slightlyBiggerBuf, 1, slightlyBiggerBuf.length);
  }

  private static byte[] randomBuffer(ThreadLocalRandom random, int size) {
    byte[] buf = new byte[size];
    random.nextBytes(buf);
    return buf;
  }

  /** Compute a random partition of @size to pairs of (offset, len). */
  private static List<OffsetLenPair> randomChunks(ThreadLocalRandom random, int size) {
    ArrayList<OffsetLenPair> chunks = new ArrayList<>();
    int offset = 0;
    while (offset != size) {
      int remaining = size - offset;
      int toWrite = random.nextInt(remaining + 1);
      chunks.add(new OffsetLenPair(offset, toWrite));
      offset += toWrite;
    }
    return chunks;
  }

  private static final class OffsetLenPair {
    final int offset;
    final int len;

    public OffsetLenPair(int offset, int len) {
      this.offset = offset;
      this.len = len;
    }
  }
}
