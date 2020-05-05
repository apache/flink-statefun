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
package org.apache.flink.statefun.flink.core.feedback;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.statefun.flink.core.logger.FeedbackLogger;
import org.apache.flink.util.Preconditions;
import org.junit.Test;

public class CheckpointsTest {

  @Test
  public void usageExample() {
    Loggers loggers = new Loggers();

    Checkpoints<String> checkpoints = new Checkpoints<>(loggers);
    checkpoints.startLogging(1, new ByteArrayOutputStream());
    checkpoints.append("hello");
    checkpoints.append("world");
    checkpoints.commitCheckpointsUntil(1);

    assertThat(loggers.items(0), contains("hello", "world"));
    assertThat(loggers.state(0), is(LoggerState.COMMITTED));
  }

  @Test
  public void dataIsAppendedToMultipleLoggers() {
    Loggers loggers = new Loggers();

    Checkpoints<String> checkpoints = new Checkpoints<>(loggers);

    checkpoints.startLogging(1, new ByteArrayOutputStream());
    checkpoints.append("a");

    checkpoints.startLogging(2, new ByteArrayOutputStream());
    checkpoints.append("b");

    checkpoints.commitCheckpointsUntil(1);
    checkpoints.append("c");

    checkpoints.commitCheckpointsUntil(2);

    assertThat(loggers.items(0), contains("a", "b"));
    assertThat(loggers.items(1), contains("b", "c"));
  }

  @Test
  public void committingALaterCheckpointCommitsPreviousCheckpoints() {
    Loggers loggers = new Loggers();

    Checkpoints<String> checkpoints = new Checkpoints<>(loggers);

    checkpoints.startLogging(1, new ByteArrayOutputStream());
    checkpoints.startLogging(2, new ByteArrayOutputStream());
    checkpoints.commitCheckpointsUntil(2);

    assertThat(loggers.state(0), is(LoggerState.COMMITTED));
    assertThat(loggers.state(1), is(LoggerState.COMMITTED));
  }

  private enum LoggerState {
    IDLE,
    LOGGING,
    COMMITTED,
    CLOSED
  }

  private static final class Loggers implements Supplier<FeedbackLogger<String>> {
    private final List<FakeLogger> loggers = new ArrayList<>();

    @Override
    public FeedbackLogger<String> get() {
      FakeLogger logger = new FakeLogger();
      loggers.add(logger);
      return logger;
    }

    List<String> items(int loggerIndex) {
      Preconditions.checkElementIndex(loggerIndex, loggers.size());
      FakeLogger logger = loggers.get(loggerIndex);
      return logger.items;
    }

    LoggerState state(int loggerIndex) {
      Preconditions.checkElementIndex(loggerIndex, loggers.size());
      FakeLogger logger = loggers.get(loggerIndex);
      return logger.state;
    }
  }

  private static final class FakeLogger implements FeedbackLogger<String> {

    List<String> items = new ArrayList<>();
    LoggerState state = LoggerState.IDLE;

    @Override
    public void startLogging(OutputStream keyedStateCheckpointOutputStream) {
      Preconditions.checkState(state == LoggerState.IDLE);
      state = LoggerState.LOGGING;
    }

    @Override
    public void append(String message) {
      Preconditions.checkState(state != LoggerState.COMMITTED);
      Preconditions.checkState(state != LoggerState.CLOSED);
      items.add(message);
    }

    @Override
    public void commit() {
      Preconditions.checkState(state == LoggerState.LOGGING);
      state = LoggerState.COMMITTED;
    }

    @Override
    public void close() {
      state = LoggerState.CLOSED;
    }
  }
}
