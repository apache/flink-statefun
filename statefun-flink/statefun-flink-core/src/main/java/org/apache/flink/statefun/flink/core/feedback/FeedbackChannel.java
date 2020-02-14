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

import java.io.Closeable;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.util.IOUtils;

/** Single producer, single consumer channel. */
public final class FeedbackChannel<T> implements Closeable {

  /** The key that used to identify this channel. */
  private final SubtaskFeedbackKey<T> key;

  /** The underlying queue used to hold the feedback results. */
  private final FeedbackQueue<T> queue;

  /** A single registered consumer */
  private final AtomicReference<ConsumerTask<T>> consumerRef = new AtomicReference<>();

  FeedbackChannel(SubtaskFeedbackKey<T> key, FeedbackQueue<T> queue) {
    this.key = Objects.requireNonNull(key);
    this.queue = Objects.requireNonNull(queue);
  }

  // --------------------------------------------------------------------------------------------------------------
  // API
  // --------------------------------------------------------------------------------------------------------------

  /** Adds a feedback result to this channel. */
  public void put(T value) {
    if (!queue.addAndCheckIfWasEmpty(value)) {
      // successfully added @value into the queue, but the queue wasn't (atomically) drained yet,
      // so there is nothing more to do.
      return;
    }
    @SuppressWarnings("resource")
    final ConsumerTask<T> consumer = consumerRef.get();
    if (consumer == null) {
      // the queue has become non empty at the first time, yet at the same time the (single)
      // consumer has not yet registered, so there is nothing to do.
      // once the consumer would register a drain would be scheduled for the first time.
      return;
    }
    // the queue was previously empty, and now it is not, therefore we schedule a drain.
    consumer.scheduleDrainAll();
  }

  /**
   * Register a feedback iteration consumer
   *
   * @param consumer the feedback events consumer.
   * @param executor the executor to schedule feedback consumption on.
   */
  void registerConsumer(final FeedbackConsumer<T> consumer, Executor executor) {
    Objects.requireNonNull(consumer);

    ConsumerTask<T> consumerTask = new ConsumerTask<>(executor, consumer, queue);

    if (!this.consumerRef.compareAndSet(null, consumerTask)) {
      throw new IllegalStateException("There can be only a single consumer in a FeedbackChannel.");
    }
    // we must try to drain the underlying queue on registration (by scheduling the consumerTask)
    // because
    // the consumer might be registered after the producer has already started producing data into
    // the feedback channel.
    consumerTask.scheduleDrainAll();
  }

  // --------------------------------------------------------------------------------------------------------------
  // Internal
  // --------------------------------------------------------------------------------------------------------------

  /** Closes this channel. */
  @Override
  public void close() {
    ConsumerTask<T> consumer = consumerRef.getAndSet(null);
    IOUtils.closeQuietly(consumer);
    // remove this channel.
    FeedbackChannelBroker broker = FeedbackChannelBroker.get();
    broker.removeChannel(key);
  }

  private static final class ConsumerTask<T> implements Runnable, Closeable {
    private final Executor executor;
    private final FeedbackConsumer<T> consumer;
    private final FeedbackQueue<T> queue;

    ConsumerTask(Executor executor, FeedbackConsumer<T> consumer, FeedbackQueue<T> queue) {
      this.executor = Objects.requireNonNull(executor);
      this.consumer = Objects.requireNonNull(consumer);
      this.queue = Objects.requireNonNull(queue);
    }

    void scheduleDrainAll() {
      executor.execute(this);
    }

    @Override
    public void run() {
      final Deque<T> buffer = queue.drainAll();
      try {
        T element;
        while ((element = buffer.pollFirst()) != null) {
          consumer.processFeedback(element);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {}
  }
}
