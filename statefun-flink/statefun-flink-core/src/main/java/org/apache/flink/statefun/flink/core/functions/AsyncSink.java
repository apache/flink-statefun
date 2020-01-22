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

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.queue.Locks;
import org.apache.flink.statefun.flink.core.queue.MpscQueue;

final class AsyncSink {
  private final MapState<Long, Message> pendingAsyncOperations;
  private final Lazy<Reductions> reductions;
  private final Executor asOperator;
  private final Executor operatorMailbox;

  private final MpscQueue<Message> completed = new MpscQueue<>(32768, Locks.jdkReentrantLock());

  @Inject
  AsyncSink(
      @Label("async-operations") MapState<Long, Message> pendingAsyncOperations,
      @Label("checkpoint-lock-executor") Executor asOperator,
      @Label("mailbox-executor") Executor operatorMailbox,
      @Label("reductions") Lazy<Reductions> reductions) {
    this.pendingAsyncOperations = Objects.requireNonNull(pendingAsyncOperations);
    this.asOperator = Objects.requireNonNull(asOperator);
    this.reductions = Objects.requireNonNull(reductions);
    this.operatorMailbox = Objects.requireNonNull(operatorMailbox);
  }

  <T> void accept(Message metadata, CompletableFuture<T> future) {
    final long futureId = ThreadLocalRandom.current().nextLong(); // TODO: is this is good enough?
    // we keep the message in state (associated with futureId) until either:
    // 1. the future successfully completes and the message is processed. The state would be
    // cleared by the AsyncMessageDecorator after a successful application.
    // 2. after recovery, we clear that state by notifying the owning function that we don't know
    // what happened
    // with that particular async operation.
    try {
      pendingAsyncOperations.put(futureId, metadata);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    future.whenComplete((result, throwable) -> enqueue(metadata, futureId, result, throwable));
  }

  private <T> void enqueue(Message message, long futureId, T result, Throwable throwable) {
    AsyncMessageDecorator<T> decoratedMessage =
        new AsyncMessageDecorator<>(pendingAsyncOperations, futureId, message, result, throwable);

    final int size = completed.add(decoratedMessage);
    if (size == 1) {
      // the queue has become non empty, we need to schedule a drain operation.
      operatorMailbox.execute(this::drainOnOperatorThreadUnderCheckpointLock);
    }
  }

  private void drainOnOperatorThreadUnderCheckpointLock() {
    asOperator.execute(
        () -> {
          Deque<Message> batchOfCompletedFutures = completed.drainAll();
          Reductions reductions = this.reductions.get();
          Message message;
          while ((message = batchOfCompletedFutures.poll()) != null) {
            reductions.enqueue(message);
          }
          reductions.processEnvelopes();
        });
  }
}
