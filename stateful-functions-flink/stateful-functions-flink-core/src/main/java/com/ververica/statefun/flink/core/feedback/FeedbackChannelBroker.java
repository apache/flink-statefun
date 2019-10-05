/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.feedback;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandOffChannelBroker.
 *
 * <p>It is used together with the co-location constrain so that two tasks can access the same
 * "hand-off" channel, and communicate directly (not via the network stack) by simply passing
 * references in one direction.
 *
 * <p>To obtain a feedback channel one must first obtain an {@link SubtaskFeedbackKey} and simply
 * call {@link #get()}. A channel is removed from this broker on a call to {@link
 * FeedbackChannel#close()}.
 */
public final class FeedbackChannelBroker {

  private static final FeedbackChannelBroker INSTANCE = new FeedbackChannelBroker();

  private final ConcurrentHashMap<SubtaskFeedbackKey<?>, FeedbackChannel<?>> channels =
      new ConcurrentHashMap<>();

  public static FeedbackChannelBroker get() {
    return INSTANCE;
  }

  @SuppressWarnings({"unchecked"})
  public <V> FeedbackChannel<V> getChannel(SubtaskFeedbackKey<V> key) {
    Objects.requireNonNull(key);

    FeedbackChannel<?> channel = channels.computeIfAbsent(key, FeedbackChannelBroker::newChannel);

    return (FeedbackChannel<V>) channel;
  }

  @SuppressWarnings("resource")
  void removeChannel(SubtaskFeedbackKey<?> key) {
    channels.remove(key);
  }

  private static <V> FeedbackChannel<V> newChannel(SubtaskFeedbackKey<V> key) {
    FeedbackQueue<V> queue = new LockFreeBatchFeedbackQueue<>();
    return new FeedbackChannel<>(key, queue);
  }
}
