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
package org.apache.flink.statefun.sdk.java.testing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

/**
 * An implementation of {@link Context} to to make it easier to test {@link
 * org.apache.flink.statefun.sdk.java.StatefulFunction}s in isolation. It can be instantiated with
 * the address of the function under test and optionally the address of the caller.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TestContext implements Context {

  public static TestContext forTarget(Address self) {
    return new TestContext(self);
  }

  public static TestContext forTargetWithCaller(Address self, Address caller) {
    return new TestContext(self, caller);
  }

  private final AddressScopedStorage storage;
  private final Address self;
  private final Optional<Address> caller;

  private final List<SideEffects.SendSideEffect> sentMessages = new ArrayList<>();
  private final List<SideEffects.SendAfterSideEffect> sentDelayedMessages = new ArrayList<>();
  private final List<SideEffects.EgressSideEffect> sentEgressMessages = new ArrayList<>();

  private TestContext(Address self, Optional<Address> caller) {
    this.self = self;
    this.caller = caller;
    this.storage = new TestAddressScopedStorage();
  }

  private TestContext(Address self) {
    this(Objects.requireNonNull(self), Optional.empty());
  }

  private TestContext(Address self, Address caller) {
    this(Objects.requireNonNull(self), Optional.of(Objects.requireNonNull(caller)));
  }

  @Override
  public Address self() {
    return self;
  }

  @Override
  public Optional<Address> caller() {
    return caller;
  }

  @Override
  public void send(Message message) {
    Message m = Objects.requireNonNull(message);
    sentMessages.add(new SideEffects.SendSideEffect(m));
  }

  @Override
  public void sendAfter(Duration duration, Message message) {
    Duration d = Objects.requireNonNull(duration);
    Message m = Objects.requireNonNull(message);
    sentDelayedMessages.add(new SideEffects.SendAfterSideEffect(d, m));
  }

  @Override
  public void sendAfter(Duration duration, String cancellationToken, Message message) {
    Duration d = Objects.requireNonNull(duration);
    Message m = Objects.requireNonNull(message);
    sentDelayedMessages.add(new SideEffects.SendAfterSideEffect(d, m, cancellationToken));
  }

  @Override
  public void cancelDelayedMessage(String cancellationToken) {
    sentDelayedMessages.removeIf(
        effect ->
            effect.cancellationToken().isPresent()
                && effect.cancellationToken().get().equals(cancellationToken));
  }

  @Override
  public void send(EgressMessage message) {
    EgressMessage m = Objects.requireNonNull(message);
    sentEgressMessages.add(new SideEffects.EgressSideEffect(m));
  }

  @Override
  public AddressScopedStorage storage() {
    return storage;
  }

  /**
   * This method returns a list of all messages sent by this function via {@link
   * Context#send(Message)} or {@link Context#sendAfter(Duration, Message)}.
   *
   * <p>Messages are wrapped in an {@link SideEffects.SendSideEffect} that contains the message
   * itself and the duration after which the message was sent. The Duration is {@link Duration#ZERO}
   * for messages sent via {@link Context#send(Message)}.
   *
   * @return the list of sent messages wrapped in {@link SideEffects.SendSideEffect}s
   */
  public List<SideEffects.SendSideEffect> getSentMessages() {
    return new ArrayList<>(sentMessages);
  }

  public List<SideEffects.SendAfterSideEffect> getSentDelayedMessages() {
    return new ArrayList<>(sentDelayedMessages);
  }

  /**
   * This method returns a list of all egress messages sent by this function via {@link
   * Context#send(EgressMessage)}.
   *
   * @return the list of sent {@link EgressMessage}s
   */
  public List<SideEffects.EgressSideEffect> getSentEgressMessages() {
    return new ArrayList<>(sentEgressMessages);
  }
}
