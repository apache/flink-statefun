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
package org.apache.flink.statefun.sdk.java;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

/**
 * A {@link Context} contains information about the current function invocation, such as the invoked
 * function instance's and caller's {@link Address}. It is also used for side-effects as a result of
 * the invocation such as send messages to other functions or egresses, and provides access to
 * {@link AddressScopedStorage} scoped to the current {@link Address}.
 */
public interface Context {

  /** @return The current invoked function instance's {@link Address}. */
  Address self();

  /**
   * @return The caller function instance's {@link Address}, if applicable. This is {@link
   *     Optional#empty()} if the message was sent to this function via an ingress.
   */
  Optional<Address> caller();

  /**
   * Sends out a {@link Message} to another function.
   *
   * @param message the message to send.
   */
  void send(Message message);

  /**
   * Sends out a {@link Message} to another function, after a specified {@link Duration} delay.
   *
   * @param duration the amount of time to delay the message delivery.
   * @param message the message to send.
   */
  void sendAfter(Duration duration, Message message);

  /**
   * Sends out a {@link Message} to another function, after a specified {@link Duration} delay.
   *
   * @param duration the amount of time to delay the message delivery. * @param cancellationToken
   * @param cancellationToken the non-empty, non-null, unique token to attach to this message, to be
   *     used for message cancellation. (see {@link #cancelDelayedMessage(String)}.)
   * @param message the message to send.
   */
  void sendAfter(Duration duration, String cancellationToken, Message message);

  /**
   * Cancel a delayed message (a message that was send via {@link #sendAfter(Duration, Message)}).
   *
   * <p>NOTE: this is a best-effort operation, since the message might have been already delivered.
   * If the message was delivered, this is a no-op operation.
   *
   * @param cancellationToken the id of the message to un-send.
   */
  void cancelDelayedMessage(String cancellationToken);

  /**
   * Sends out a {@link EgressMessage} to an egress.
   *
   * @param message the message to send.
   */
  void send(EgressMessage message);

  /**
   * @return The {@link AddressScopedStorage}, providing access to stored values scoped to the
   *     current invoked function instance's {@link Address} (which is obtainable using {@link
   *     #self()}).
   */
  AddressScopedStorage storage();

  default CompletableFuture<Void> done() {
    return CompletableFuture.completedFuture(null);
  }
}
