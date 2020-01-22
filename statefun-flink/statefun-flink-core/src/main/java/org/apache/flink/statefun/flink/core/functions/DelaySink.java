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
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.util.Preconditions;

final class DelaySink implements Triggerable<String, VoidNamespace> {

  private final InternalTimerService<VoidNamespace> delayedMessagesTimerService;
  private final DelayedMessagesBuffer delayedMessagesBuffer;

  private final Lazy<Reductions> reductionsSupplier;
  private final Partition thisPartition;
  private final RemoteSink remoteSink;

  @Inject
  DelaySink(
      @Label("delayed-messages-buffer") DelayedMessagesBuffer delayedMessagesBuffer,
      @Label("delayed-messages-timer-service-factory")
          TimerServiceFactory delayedMessagesTimerServiceFactory,
      @Label("reductions") Lazy<Reductions> reductionsSupplier,
      Partition thisPartition,
      RemoteSink remoteSink) {
    this.delayedMessagesBuffer = Objects.requireNonNull(delayedMessagesBuffer);
    this.reductionsSupplier = Objects.requireNonNull(reductionsSupplier);
    this.thisPartition = Objects.requireNonNull(thisPartition);
    this.remoteSink = Objects.requireNonNull(remoteSink);

    this.delayedMessagesTimerService = delayedMessagesTimerServiceFactory.createTimerService(this);
  }

  void accept(Message message, long delayMillis) {
    Objects.requireNonNull(message);
    Preconditions.checkArgument(delayMillis >= 0);

    final long triggerTime = delayedMessagesTimerService.currentProcessingTime() + delayMillis;

    delayedMessagesTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, triggerTime);
    delayedMessagesBuffer.add(message, triggerTime);
  }

  @Override
  public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
    final long triggerTimestamp = timer.getTimestamp();
    final Reductions reductions = reductionsSupplier.get();

    Iterable<Message> delayedMessages = delayedMessagesBuffer.getForTimestamp(triggerTimestamp);
    if (delayedMessages == null) {
      throw new IllegalStateException(
          "A delayed message timer was triggered with timestamp "
              + triggerTimestamp
              + ", but no messages were buffered for it.");
    }
    for (Message delayedMessage : delayedMessages) {
      if (thisPartition.contains(delayedMessage.target())) {
        reductions.enqueue(delayedMessage);
      } else {
        remoteSink.accept(delayedMessage);
      }
    }
    // we clear the delayedMessageBuffer *before* we process the enqueued local reductions, because
    // processing the envelops might actually trigger a delayed message to be sent with the same
    // @triggerTimestamp
    // so it would be re-enqueued into the delayedMessageBuffer.
    delayedMessagesBuffer.clearForTimestamp(triggerTimestamp);
    reductions.processEnvelopes();
  }

  @Override
  public void onEventTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
    throw new UnsupportedOperationException(
        "Delayed messages with event time semantics is not supported.");
  }
}
