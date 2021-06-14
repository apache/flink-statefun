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
package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.getTypedValue;
import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.protoAddressFromSdk;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;

/**
 * A thread safe implementation of a {@linkplain Context}.
 *
 * <p>This context's life cycle is tied to a single batch request. It is constructed when a
 * {@linkplain org.apache.flink.statefun.sdk.reqreply.generated.ToFunction} message arrives, and it
 * carries enough context to compute an {@linkplain
 * org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.InvocationResponse}. Access to the
 * send/sendAfter/sendEgress methods are synchronized with a @responseBuilder's lock, to prevent
 * concurrent modification. When the last invocation of the batch completes successfully, a {@link
 * #finalBuilder()} will be called. After that point no further operations are allowed.
 */
final class ConcurrentContext implements Context {
  private final org.apache.flink.statefun.sdk.java.Address self;
  private final FromFunction.InvocationResponse.Builder responseBuilder;
  private final ConcurrentAddressScopedStorage storage;
  private boolean noFurtherModificationsAllowed;

  private Address caller;

  public ConcurrentContext(
      org.apache.flink.statefun.sdk.java.Address self,
      FromFunction.InvocationResponse.Builder responseBuilder,
      ConcurrentAddressScopedStorage storage) {
    this.self = Objects.requireNonNull(self);
    this.responseBuilder = Objects.requireNonNull(responseBuilder);
    this.storage = Objects.requireNonNull(storage);
  }

  @Override
  public org.apache.flink.statefun.sdk.java.Address self() {
    return self;
  }

  void setCaller(Address address) {
    this.caller = address;
  }

  FromFunction.InvocationResponse.Builder finalBuilder() {
    synchronized (responseBuilder) {
      noFurtherModificationsAllowed = true;
      return responseBuilder;
    }
  }

  @Override
  public Optional<Address> caller() {
    return Optional.ofNullable(caller);
  }

  @Override
  public void send(Message message) {
    Objects.requireNonNull(message);

    FromFunction.Invocation outInvocation =
        FromFunction.Invocation.newBuilder()
            .setArgument(getTypedValue(message))
            .setTarget(protoAddressFromSdk(message.targetAddress()))
            .build();

    synchronized (responseBuilder) {
      checkNotDone();
      responseBuilder.addOutgoingMessages(outInvocation);
    }
  }

  @Override
  public void sendAfter(Duration duration, Message message) {
    Objects.requireNonNull(duration);
    Objects.requireNonNull(message);

    FromFunction.DelayedInvocation outInvocation =
        FromFunction.DelayedInvocation.newBuilder()
            .setArgument(getTypedValue(message))
            .setTarget(protoAddressFromSdk(message.targetAddress()))
            .setDelayInMs(duration.toMillis())
            .build();

    synchronized (responseBuilder) {
      checkNotDone();
      responseBuilder.addDelayedInvocations(outInvocation);
    }
  }

  @Override
  public void sendAfter(Duration duration, String cancellationToken, Message message) {
    Objects.requireNonNull(duration);
    if (cancellationToken == null || cancellationToken.isEmpty()) {
      throw new IllegalArgumentException("message cancellation token can not be empty or null.");
    }
    Objects.requireNonNull(message);

    FromFunction.DelayedInvocation outInvocation =
        FromFunction.DelayedInvocation.newBuilder()
            .setArgument(getTypedValue(message))
            .setTarget(protoAddressFromSdk(message.targetAddress()))
            .setDelayInMs(duration.toMillis())
            .setCancellationToken(cancellationToken)
            .build();

    synchronized (responseBuilder) {
      checkNotDone();
      responseBuilder.addDelayedInvocations(outInvocation);
    }
  }

  @Override
  public void cancelDelayedMessage(String cancellationToken) {
    if (cancellationToken == null || cancellationToken.isEmpty()) {
      throw new IllegalArgumentException("message cancellation token can not be empty or null.");
    }

    FromFunction.DelayCancellation cancellation =
        FromFunction.DelayCancellation.newBuilder().setCancellationToken(cancellationToken).build();

    synchronized (responseBuilder) {
      checkNotDone();
      responseBuilder.addOutgoingDelayCancellations(cancellation);
    }
  }

  @Override
  public void send(EgressMessage message) {
    Objects.requireNonNull(message);

    TypeName target = message.targetEgressId();

    FromFunction.EgressMessage outInvocation =
        FromFunction.EgressMessage.newBuilder()
            .setArgument(getTypedValue(message))
            .setEgressNamespace(target.namespace())
            .setEgressType(target.name())
            .build();

    synchronized (responseBuilder) {
      checkNotDone();
      responseBuilder.addOutgoingEgresses(outInvocation);
    }
  }

  @Override
  public AddressScopedStorage storage() {
    return storage;
  }

  private void checkNotDone() {
    if (noFurtherModificationsAllowed) {
      throw new IllegalStateException("Function has already completed its execution.");
    }
  }
}
