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
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.statefun.flink.core.di.ObjectContainer;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("SameParameterValue")
public class UnboundedFeedbackLoggerTest {
  private static IOManagerAsync IO_MANAGER;

  @BeforeClass
  public static void beforeClass() {
    IO_MANAGER = new IOManagerAsync();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (IO_MANAGER != null) {
      IO_MANAGER.close();
      IO_MANAGER = null;
    }
  }

  @Test
  public void sanity() {
    UnboundedFeedbackLogger<Integer> logger = instanceUnderTest(128, 1);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    logger.startLogging(output);
    logger.commit();

    assertThat(output.size(), Matchers.greaterThan(0));
  }

  @Test(expected = IllegalStateException.class)
  public void commitWithoutStartLoggingShouldBeIllegal() {
    UnboundedFeedbackLogger<Integer> logger = instanceUnderTest(128, 1);

    logger.commit();
  }

  @Test
  public void roundTrip() throws Exception {
    roundTrip(100, 1024);
  }

  @Ignore
  @Test
  public void roundTripWithSpill() throws Exception {
    roundTrip(1_000_000, 0);
  }

  private void roundTrip(int numElements, int maxMemoryInBytes) throws Exception {
    InputStream input = serializeKeyGroup(1, maxMemoryInBytes, numElements);

    ArrayList<Integer> messages = new ArrayList<>();

    UnboundedFeedbackLogger<Integer> loggerUnderTest = instanceUnderTest(1, 0);
    loggerUnderTest.replyLoggedEnvelops(input, messages::add);

    for (int i = 0; i < numElements; i++) {
      Integer message = messages.get(i);
      assertThat(message, is(i));
    }
  }

  private ByteArrayInputStream serializeKeyGroup(int maxParallelism, long maxMemory, int numItems) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    UnboundedFeedbackLogger<Integer> loggerUnderTest = instanceUnderTest(maxParallelism, maxMemory);

    loggerUnderTest.startLogging(output);

    for (int i = 0; i < numItems; i++) {
      loggerUnderTest.append(i);
    }

    loggerUnderTest.commit();

    return new ByteArrayInputStream(output.toByteArray());
  }

  @SuppressWarnings("unchecked")
  private UnboundedFeedbackLogger<Integer> instanceUnderTest(int maxParallelism, long totalMemory) {

    ObjectContainer container =
        Loggers.unboundedSpillableLoggerContainer(
            IO_MANAGER, maxParallelism, totalMemory, IntSerializer.INSTANCE, Function.identity());

    container.add(
        "checkpoint-stream-ops",
        CheckpointedStreamOperations.class,
        new NoopStreamOps(maxParallelism));
    return container.get(UnboundedFeedbackLoggerFactory.class).create();
  }

  static final class NoopStreamOps implements CheckpointedStreamOperations {
    private final int maxParallelism;

    NoopStreamOps(int maxParallelism) {
      this.maxParallelism = maxParallelism;
    }

    @Override
    public void requireKeyedStateCheckpointed(OutputStream keyedStateCheckpointOutputStream) {
      // noop
    }

    @Override
    public Iterable<Integer> keyGroupList(OutputStream stream) {
      IntStream range = IntStream.range(0, maxParallelism);
      return range::iterator;
    }

    @Override
    public void startNewKeyGroup(OutputStream stream, int keyGroup) {}

    @Override
    public Closeable acquireLease(OutputStream keyedStateCheckpointOutputStream) {
      return () -> {}; // NOOP
    }
  }
}
