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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;

final class FlinkStateDelayedMessagesBuffer implements DelayedMessagesBuffer {

  static final String BUFFER_STATE_NAME = "delayed-messages-buffer";

  private final InternalListState<String, Long, Message> bufferState;

  @Inject
  FlinkStateDelayedMessagesBuffer(
      @Label("delayed-messages-buffer-state")
          InternalListState<String, Long, Message> bufferState) {
    this.bufferState = Objects.requireNonNull(bufferState);
  }

  @Override
  public void add(Message message, long untilTimestamp) {
    bufferState.setCurrentNamespace(untilTimestamp);
    try {
      bufferState.add(message);
    } catch (Exception e) {
      throw new RuntimeException("Error adding delayed message to state buffer: " + message, e);
    }
  }

  @Override
  public Iterable<Message> getForTimestamp(long timestamp) {
    bufferState.setCurrentNamespace(timestamp);

    try {
      return bufferState.get();
    } catch (Exception e) {
      throw new RuntimeException(
          "Error accessing delayed message in state buffer for timestamp: " + timestamp, e);
    }
  }

  @Override
  public void clearForTimestamp(long timestamp) {
    bufferState.setCurrentNamespace(timestamp);
    bufferState.clear();
  }
}
