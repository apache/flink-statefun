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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;

final class FlinkStateDelayedMessagesBuffer implements DelayedMessagesBuffer {

  static final String BUFFER_STATE_NAME = "delayed-messages-buffer";
  static final String INDEX_STATE_NAME = "delayed-message-index";

  private final InternalListState<String, Long, Message> bufferState;
  private final MapState<String, Long> cancellationTokenToTimestamp;

  @Inject
  FlinkStateDelayedMessagesBuffer(
      @Label("delayed-messages-buffer-state") InternalListState<String, Long, Message> bufferState,
      @Label("delayed-message-index") MapState<String, Long> cancellationTokenToTimestamp) {
    this.bufferState = Objects.requireNonNull(bufferState);
    this.cancellationTokenToTimestamp = Objects.requireNonNull(cancellationTokenToTimestamp);
  }

  @Override
  public void forEachMessageAt(long timestamp, Consumer<Message> fn) {
    try {
      forEachMessageThrows(timestamp, fn);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public OptionalLong removeMessageByCancellationToken(String token) {
    try {
      return remove(token);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed clearing a message with a cancellation token " + token, e);
    }
  }

  @Override
  public void add(Message message, long untilTimestamp) {
    try {
      addThrows(message, untilTimestamp);
    } catch (Exception e) {
      throw new RuntimeException("Error adding delayed message to state buffer: " + message, e);
    }
  }

  // -----------------------------------------------------------------------------------------------------
  // Internal
  // -----------------------------------------------------------------------------------------------------

  private void forEachMessageThrows(long timestamp, Consumer<Message> fn) throws Exception {
    bufferState.setCurrentNamespace(timestamp);
    for (Message message : bufferState.get()) {
      removeMessageIdMapping(message);
      fn.accept(message);
    }
    bufferState.clear();
  }

  private void addThrows(Message message, long untilTimestamp) throws Exception {
    bufferState.setCurrentNamespace(untilTimestamp);
    bufferState.add(message);
    Optional<String> maybeToken = message.cancellationToken();
    if (!maybeToken.isPresent()) {
      return;
    }
    String cancellationToken = maybeToken.get();
    @Nullable Long previousTimestamp = cancellationTokenToTimestamp.get(cancellationToken);
    if (previousTimestamp != null) {
      throw new IllegalStateException(
          "Trying to associate a message with cancellation token "
              + cancellationToken
              + " and timestamp "
              + untilTimestamp
              + ", but a message with the same cancellation token exists and with a timestamp "
              + previousTimestamp);
    }
    cancellationTokenToTimestamp.put(cancellationToken, untilTimestamp);
  }

  private OptionalLong remove(String cancellationToken) throws Exception {
    final @Nullable Long untilTimestamp = cancellationTokenToTimestamp.get(cancellationToken);
    if (untilTimestamp == null) {
      // The message associated with @cancellationToken has already been delivered, or previously
      // removed.
      return OptionalLong.empty();
    }
    cancellationTokenToTimestamp.remove(cancellationToken);
    bufferState.setCurrentNamespace(untilTimestamp);
    List<Message> newList = removeMessageByToken(bufferState.get(), cancellationToken);
    if (!newList.isEmpty()) {
      // There are more messages to process, so we indicate to the caller that
      // they should NOT cancel the timer.
      bufferState.update(newList);
      return OptionalLong.empty();
    }
    // There are no more message to remove, we clear the buffer and indicate
    // to our caller to remove the timer for @untilTimestamp
    bufferState.clear();
    return OptionalLong.of(untilTimestamp);
  }

  // ---------------------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------------------

  private void removeMessageIdMapping(Message message) throws Exception {
    Optional<String> maybeToken = message.cancellationToken();
    if (maybeToken.isPresent()) {
      cancellationTokenToTimestamp.remove(maybeToken.get());
    }
  }

  private static List<Message> removeMessageByToken(Iterable<Message> messages, String token) {
    ArrayList<Message> newList = new ArrayList<>();
    for (Message message : messages) {
      Optional<String> thisMessageId = message.cancellationToken();
      if (!thisMessageId.isPresent() || !Objects.equals(thisMessageId.get(), token)) {
        newList.add(message);
      }
    }
    return newList;
  }
}
