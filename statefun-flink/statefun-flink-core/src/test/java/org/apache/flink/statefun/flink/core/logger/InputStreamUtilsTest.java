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

package org.apache.flink.statefun.flink.core.logger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.InputStream;
import java.util.Arrays;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.Test;

public final class InputStreamUtilsTest {

  private static final int NUM_REPEAT_TESTS = 10000;

  @Test
  public void tryReadFullyExampleUsage() throws Exception {
    final byte[] testBytes = "test-data".getBytes();
    final byte[] readBuffer = new byte[testBytes.length];

    try (InputStream in = testInputStream(testBytes)) {
      final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

      assertThat(numReadBytes, is(testBytes.length));
      assertThat(readBuffer, is(testBytes));
      assertThat(in.read(), is(-1));
    }
  }

  @Test
  public void tryReadFullyEmptyInputStream() throws Exception {
    repeatTest(
        NUM_REPEAT_TESTS,
        () -> {
          final byte[] testBytes = new byte[0];
          final byte[] readBuffer = new byte[10];

          try (InputStream in = testInputStream(testBytes)) {
            final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

            assertThat(numReadBytes, is(0));
            assertThat(readBuffer, is(new byte[10]));
            assertThat(in.read(), is(-1));
          }
        });
  }

  @Test
  public void tryReadFullyReadBufferSizeLargerThanInputStream() throws Exception {
    repeatTest(
        NUM_REPEAT_TESTS,
        () -> {
          final byte[] testBytes = new byte[] {-91, 11, 8};
          // read buffer has larger size than the test data
          final byte[] readBuffer = new byte[testBytes.length + 20];

          try (InputStream in = testInputStream(testBytes)) {
            final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

            assertThat(numReadBytes, is(testBytes.length));
            assertThat(readBuffer, is(Arrays.copyOf(testBytes, readBuffer.length)));
            assertThat(in.read(), is(-1));
          }
        });
  }

  @Test
  public void tryReadFullyReadBufferSizeSmallerThanInputStream() throws Exception {
    repeatTest(
        NUM_REPEAT_TESTS,
        () -> {
          final byte[] testBytes = new byte[] {-91, 11, 8, 53, 100, 5, -100, 102, 56, 95};
          // read buffer has smaller size than the test data
          final byte[] readBuffer = new byte[testBytes.length - 2];

          try (InputStream in = testInputStream(testBytes)) {
            final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

            assertThat(numReadBytes, is(readBuffer.length));
            assertThat(readBuffer, is(Arrays.copyOfRange(testBytes, 0, readBuffer.length)));

            // assert that the input stream is not overly-read
            assertThat(in.read(), is(56));
            assertThat(in.read(), is(95));
            assertThat(in.read(), is(-1));
          }
        });
  }

  @Test
  public void tryReadFullyEmptyReadBuffer() throws Exception {
    repeatTest(
        NUM_REPEAT_TESTS,
        () -> {
          final byte[] testBytes = "test-data".getBytes();
          final byte[] readBuffer = new byte[0];

          try (InputStream in = testInputStream(testBytes)) {
            final int numReadBytes = InputStreamUtils.tryReadFully(in, readBuffer);

            assertThat(numReadBytes, is(0));
            assertThat(readBuffer, is(new byte[0]));
          }
        });
  }

  private static InputStream testInputStream(byte[] streamBytes) {
    return new RandomReadLengthByteArrayInputStream(Arrays.copyOf(streamBytes, streamBytes.length));
  }

  private static void repeatTest(int numRepeats, ThrowingRunnable<Exception> test)
      throws Exception {
    for (int i = 0; i < numRepeats; i++) {
      test.run();
    }
  }
}
