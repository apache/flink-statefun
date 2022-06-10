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
import java.util.OptionalLong;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.util.Preconditions;

final class DelaySink implements Triggerable<String, VoidNamespace> {

  private final InternalTimerService<VoidNamespace> delayedMessagesTimerService;
  private final DelayedMessagesBuffer delayedMessagesBuffer;
  private final DelayMessageHandler delayMessageHandler;

  @Inject
  DelaySink(
      @Label("delayed-messages-buffer") DelayedMessagesBuffer delayedMessagesBuffer,
      @Label("delayed-messages-timer-service-factory")
          TimerServiceFactory delayedMessagesTimerServiceFactory,
      DelayMessageHandler delayMessageHandler) {
    this.delayedMessagesBuffer = Objects.requireNonNull(delayedMessagesBuffer);
    this.delayedMessagesTimerService = delayedMessagesTimerServiceFactory.createTimerService(this);
    this.delayMessageHandler = Objects.requireNonNull(delayMessageHandler);
  }

  void accept(Message message, long delayMillis) {
    Objects.requireNonNull(message);
    Preconditions.checkArgument(delayMillis >= 0);

    final long triggerTime = delayedMessagesTimerService.currentProcessingTime() + delayMillis;

    delayedMessagesTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, triggerTime);
    delayedMessagesBuffer.add(message, triggerTime);
  }

  @Override
  public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) {
    delayMessageHandler.onStart();
    delayedMessagesBuffer.forEachMessageAt(timer.getTimestamp(), delayMessageHandler);
    delayMessageHandler.onComplete();
  }

  @Override
  public void onEventTime(InternalTimer<String, VoidNamespace> timer) {
    throw new UnsupportedOperationException(
        "Delayed messages with event time semantics is not supported.");
  }

  void removeMessageByCancellationToken(String cancellationToken) {
    Objects.requireNonNull(cancellationToken);
    OptionalLong timerToClear =
        delayedMessagesBuffer.removeMessageByCancellationToken(cancellationToken);
    if (timerToClear.isPresent()) {
      long timestamp = timerToClear.getAsLong();
      delayedMessagesTimerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }
  }
}
