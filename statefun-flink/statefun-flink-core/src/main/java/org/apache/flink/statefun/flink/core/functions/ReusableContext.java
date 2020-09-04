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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.backpressure.AsyncWaiter;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

final class ReusableContext implements ApplyingContext, AsyncWaiter {
  private final Partition thisPartition;
  private final LocalSink localSink;
  private final RemoteSink remoteSink;
  private final DelaySink delaySink;
  private final AsyncSink asyncSink;
  private final SideOutputSink sideOutputSink;
  private final State state;
  private final MessageFactory messageFactory;

  private Message in;
  private LiveFunction function;

  @Inject
  ReusableContext(
      Partition partition,
      LocalSink localSink,
      RemoteSink remoteSink,
      DelaySink delaySink,
      AsyncSink asyncSink,
      SideOutputSink sideoutputSink,
      @Label("state") State state,
      MessageFactory messageFactory) {

    this.thisPartition = Objects.requireNonNull(partition);
    this.localSink = Objects.requireNonNull(localSink);
    this.remoteSink = Objects.requireNonNull(remoteSink);
    this.delaySink = Objects.requireNonNull(delaySink);
    this.sideOutputSink = Objects.requireNonNull(sideoutputSink);
    this.state = Objects.requireNonNull(state);
    this.messageFactory = Objects.requireNonNull(messageFactory);
    this.asyncSink = Objects.requireNonNull(asyncSink);
  }

  @Override
  public void apply(LiveFunction function, Message inMessage) {
    this.in = inMessage;
    this.function = function;
    state.setCurrentKey(inMessage.target());
    function.metrics().incomingMessage();
    function.receive(this, in);
    in.postApply();
    this.in = null;
  }

  @Override
  public void send(Address to, Object what) {
    Objects.requireNonNull(to);
    Objects.requireNonNull(what);
    Message envelope = messageFactory.from(self(), to, what);
    if (thisPartition.contains(to)) {
      localSink.accept(envelope);
      function.metrics().outgoingLocalMessage();
    } else {
      remoteSink.accept(envelope);
      function.metrics().outgoingRemoteMessage();
    }
  }

  @Override
  public <T> void send(EgressIdentifier<T> egress, T what) {
    Objects.requireNonNull(egress);
    Objects.requireNonNull(what);

    function.metrics().outgoingEgressMessage();
    sideOutputSink.accept(egress, what);
  }

  @Override
  public void sendAfter(Duration delay, Address to, Object message) {
    Objects.requireNonNull(delay);
    Objects.requireNonNull(to);
    Objects.requireNonNull(message);

    Message envelope = messageFactory.from(self(), to, message);
    delaySink.accept(envelope, delay.toMillis());
  }

  @Override
  public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {
    Objects.requireNonNull(metadata);
    Objects.requireNonNull(future);

    Message message = messageFactory.from(self(), self(), metadata);
    asyncSink.accept(self(), message, future);
  }

  @Override
  public void awaitAsyncOperationComplete() {
    asyncSink.blockAddress(self());
  }

  @Override
  public Address caller() {
    return in.source();
  }

  @Override
  public Address self() {
    return in.target();
  }
}
