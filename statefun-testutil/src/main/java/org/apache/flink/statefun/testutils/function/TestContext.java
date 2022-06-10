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
package org.apache.flink.statefun.testutils.function;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.metrics.Counter;
import org.apache.flink.statefun.sdk.metrics.Metrics;

/** A simple context that is strictly synchronous and captures all responses. */
class TestContext implements Context {

  private final Address selfAddress;

  private final StatefulFunction function;

  private final Map<EgressIdentifier<?>, List<Object>> outputs;

  private final Queue<Envelope> messages;

  private final PriorityQueue<PendingMessage> pendingMessage;

  private Map<Address, List<Object>> responses;

  private Address from;

  private long watermark;

  TestContext(Address selfAddress, StatefulFunction function, Instant startTime) {
    this.selfAddress = Objects.requireNonNull(selfAddress);
    this.function = Objects.requireNonNull(function);

    this.watermark = startTime.toEpochMilli();
    this.messages = new ArrayDeque<>();
    this.pendingMessage = new PriorityQueue<>(Comparator.comparingLong(a -> a.timer));
    this.outputs = new HashMap<>();
  }

  @Override
  public Address self() {
    return selfAddress;
  }

  @Override
  public Address caller() {
    return from;
  }

  @Override
  public void reply(Object message) {
    Address to = caller();
    if (to == null) {
      throw new IllegalStateException("The caller address is null");
    }

    send(to, message);
  }

  @Override
  public void send(Address to, Object message) {
    if (to.equals(selfAddress)) {
      messages.add(new Envelope(self(), to, message));
    }
    responses.computeIfAbsent(to, ignore -> new ArrayList<>()).add(message);
  }

  @Override
  public <T> void send(EgressIdentifier<T> egress, T message) {
    outputs.computeIfAbsent(egress, ignore -> new ArrayList<>()).add(message);
  }

  @Override
  public void sendAfter(Duration delay, Address to, Object message) {
    pendingMessage.add(
        new PendingMessage(new Envelope(self(), to, message), watermark + delay.toMillis(), null));
  }

  @Override
  public void sendAfter(Duration delay, Address to, Object message, String cancellationToken) {
    Objects.requireNonNull(cancellationToken);
    pendingMessage.add(
        new PendingMessage(
            new Envelope(self(), to, message), watermark + delay.toMillis(), cancellationToken));
  }

  @Override
  public void cancelDelayedMessage(String cancellationToken) {
    pendingMessage.removeIf(
        pendingMessage -> Objects.equals(pendingMessage.cancellationToken, cancellationToken));
  }

  @Override
  public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {
    T value = null;
    Throwable error = null;

    try {
      value = future.get();
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to get results from async action", e);
    } catch (ExecutionException e) {
      error = e.getCause();
    }

    AsyncOperationResult.Status status;
    if (error == null) {
      status = AsyncOperationResult.Status.SUCCESS;
    } else {
      status = AsyncOperationResult.Status.FAILURE;
    }

    AsyncOperationResult<M, T> result = new AsyncOperationResult<>(metadata, status, value, error);
    messages.add(new Envelope(self(), self(), result));
  }

  @Override
  public Metrics metrics() {
    // return a NOOP metrics
    return name ->
        new Counter() {
          @Override
          public void inc(long amount) {}

          @Override
          public void dec(long amount) {}
        };
  }

  @SuppressWarnings("unchecked")
  <T> List<T> getEgress(EgressIdentifier<T> identifier) {
    List<?> values = outputs.getOrDefault(identifier, Collections.emptyList());

    // Because the type is part of the identifier key
    // this cast is always safe.
    return (List<T>) values;
  }

  Map<Address, List<Object>> invoke(Address from, Object message) {
    messages.add(new Envelope(from, null, message));

    return processAllMessages();
  }

  Map<Address, List<Object>> tick(Duration duration) {
    watermark += duration.toMillis();

    while (!pendingMessage.isEmpty() && pendingMessage.peek().timer <= watermark) {
      messages.add(pendingMessage.poll().envelope);
    }

    return processAllMessages();
  }

  private Map<Address, List<Object>> processAllMessages() {
    responses = new HashMap<>();

    while (!messages.isEmpty()) {
      Envelope envelope = messages.poll();
      if (envelope.to != null && !envelope.to.equals(self())) {
        send(envelope.to, envelope.message);
      } else {
        from = envelope.from;
        function.invoke(this, envelope.message);
      }
    }

    return responses;
  }

  private static class Envelope {
    Address from;

    Address to;

    Object message;

    Envelope(Address from, Address to, Object message) {
      this.from = from;
      this.to = to;
      this.message = message;
    }
  }

  private static class PendingMessage {
    Envelope envelope;
    String cancellationToken;
    long timer;

    PendingMessage(Envelope envelope, long timer, String cancellationToken) {
      this.envelope = envelope;
      this.timer = timer;
      this.cancellationToken = cancellationToken;
    }
  }
}
