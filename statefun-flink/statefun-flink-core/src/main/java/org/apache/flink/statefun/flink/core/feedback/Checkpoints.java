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

import java.io.OutputStream;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLogger;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLoggerFactory;
import org.apache.flink.util.IOUtils;

final class Checkpoints<T> implements AutoCloseable {
  private final UnboundedFeedbackLoggerFactory<T> feedbackLoggerFactory;
  private final TreeMap<Long, UnboundedFeedbackLogger<T>> uncompletedCheckpoints = new TreeMap<>();

  Checkpoints(UnboundedFeedbackLoggerFactory<T> feedbackLoggerFactory) {
    this.feedbackLoggerFactory = Objects.requireNonNull(feedbackLoggerFactory);
  }

  public void startLogging(long checkpointId, OutputStream outputStream) {
    UnboundedFeedbackLogger<T> logger = feedbackLoggerFactory.create();
    logger.startLogging(outputStream);
    uncompletedCheckpoints.put(checkpointId, logger);
  }

  public void append(T element) {
    for (UnboundedFeedbackLogger<T> logger : uncompletedCheckpoints.values()) {
      logger.append(element);
    }
  }

  public void commitCheckpointsUntil(long checkpointId) {
    SortedMap<Long, UnboundedFeedbackLogger<T>> completedCheckpoints =
        uncompletedCheckpoints.headMap(checkpointId, true);
    completedCheckpoints.values().forEach(UnboundedFeedbackLogger::commit);
    completedCheckpoints.clear();
  }

  @Override
  public void close() {
    IOUtils.closeAllQuietly(uncompletedCheckpoints.values());
    uncompletedCheckpoints.clear();
  }
}
