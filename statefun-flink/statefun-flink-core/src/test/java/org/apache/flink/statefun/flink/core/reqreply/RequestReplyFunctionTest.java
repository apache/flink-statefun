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
package org.apache.flink.statefun.flink.core.reqreply;

import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_1_ADDR;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.DelayedInvocation;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.IncompleteInvocationContext;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation.MutationType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.Invocation;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.Test;

public class RequestReplyFunctionTest {
  private static final FunctionType FN_TYPE = new FunctionType("foo", "bar");

  private final FakeClient client = new FakeClient();
  private final FakeContext context = new FakeContext();

  private final RequestReplyFunction functionUnderTest =
      new RequestReplyFunction(
          FN_TYPE, testInitialRegisteredState("session", "com.foo.bar/myType"), 10, client, true);

  @Test
  public void example() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    assertTrue(client.wasSentToFunction.hasInvocation());
    assertThat(client.capturedInvocationBatchSize(), is(1));
  }

  @Test
  public void callerIsSet() {
    context.caller = FUNCTION_1_ADDR;
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    Invocation anInvocation = client.capturedInvocation(0);
    Address caller = polyglotAddressToSdkAddress(anInvocation.getCaller());

    assertThat(caller, is(FUNCTION_1_ADDR));
  }

  @Test
  public void messageIsSet() {
    TypedValue argument =
        TypedValue.newBuilder()
            .setTypename("io.statefun.foo/bar")
            .setHasValue(true)
            .setValue(ByteString.copyFromUtf8("Hello!"))
            .build();

    functionUnderTest.invoke(context, argument);

    assertThat(client.capturedInvocation(0).getArgument(), is(argument));
  }

  @Test
  public void batchIsAccumulatedWhileARequestIsInFlight() {
    // send one message
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    // the following invocations should be queued and sent as a batch
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // simulate a successful completion of the first operation
    functionUnderTest.invoke(context, successfulAsyncOperation());

    assertThat(client.capturedInvocationBatchSize(), is(2));
  }

  @Test
  public void reachingABatchLimitTriggersBackpressure() {
    RequestReplyFunction functionUnderTest = new RequestReplyFunction(FN_TYPE, 2, client);

    // send one message
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    // the following invocations should be queued
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // the following invocations should request backpressure
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    assertThat(context.needsWaiting, is(true));
  }

  @Test
  public void returnedMessageReleaseBackpressure() {
    RequestReplyFunction functionUnderTest = new RequestReplyFunction(FN_TYPE, 2, client);

    // the following invocations should cause backpressure
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // complete one message, should send a batch of size 3
    context.needsWaiting = false;
    functionUnderTest.invoke(context, successfulAsyncOperation());

    // the next message should not cause backpressure.
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    assertThat(context.needsWaiting, is(false));
  }

  @Test
  public void stateIsModified() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // A message returned from the function
    // that asks to put "hello" into the session state.
    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addStateMutations(
                        PersistedValueMutation.newBuilder()
                            .setStateValue(
                                TypedValue.newBuilder()
                                    .setTypename("com.foo.bar/myType")
                                    .setHasValue(true)
                                    .setValue(ByteString.copyFromUtf8("hello")))
                            .setMutationType(MutationType.MODIFY)
                            .setStateName("session")))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    assertThat(client.capturedState(0).getValue(), is(ByteString.copyFromUtf8("hello")));
  }

  @Test
  public void delayedMessages() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addDelayedInvocations(
                        DelayedInvocation.newBuilder()
                            .setArgument(TypedValue.getDefaultInstance())
                            .setDelayInMs(1)
                            .build()))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.delayed.isEmpty());
    assertEquals(Duration.ofMillis(1), context.delayed.get(0).delay());
  }

  @Test
  public void delayedMessagesWithCancellationToken() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addDelayedInvocations(
                        DelayedInvocation.newBuilder()
                            .setArgument(TypedValue.getDefaultInstance())
                            .setDelayInMs(1)
                            .setCancellationToken("foo")
                            .build()))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.delayed.isEmpty());
    assertEquals(Duration.ofMillis(1), context.delayed.get(0).delay());
    assertEquals("foo", context.delayed.get(0).cancellationToken);
  }

  @Test
  public void delayedMessagesCancellation() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addDelayedInvocations(
                        DelayedInvocation.newBuilder()
                            .setIsCancellationRequest(true)
                            .setCancellationToken("foo")
                            .build()))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.delayCancellations.isEmpty());
    assertEquals("foo", context.delayCancellations.get(0));
  }

  @Test
  public void egressIsSent() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    FromFunction response =
        FromFunction.newBuilder()
            .setInvocationResult(
                InvocationResponse.newBuilder()
                    .addOutgoingEgresses(
                        EgressMessage.newBuilder()
                            .setArgument(TypedValue.getDefaultInstance())
                            .setEgressNamespace("org.foo")
                            .setEgressType("bar")))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(response));

    assertFalse(context.egresses.isEmpty());
    assertEquals(
        new EgressIdentifier<>("org.foo", "bar", TypedValue.class),
        context.egresses.get(0).getKey());
  }

  @Test
  public void retryBatchOnIncompleteInvocationContextResponse() {
    TypedValue argument =
        TypedValue.newBuilder()
            .setTypename("io.statefun.foo/bar")
            .setValue(ByteString.copyFromUtf8("Hello!"))
            .build();
    functionUnderTest.invoke(context, argument);

    FromFunction response =
        FromFunction.newBuilder()
            .setIncompleteInvocationContext(
                IncompleteInvocationContext.newBuilder()
                    .addMissingValues(
                        PersistedValueSpec.newBuilder()
                            .setStateName("new-state")
                            .setExpirationSpec(
                                ExpirationSpec.newBuilder()
                                    .setMode(ExpirationSpec.ExpireMode.AFTER_INVOKE)
                                    .setExpireAfterMillis(5000)
                                    .build())))
            .build();

    functionUnderTest.invoke(context, successfulAsyncOperation(client.wasSentToFunction, response));

    // re-sent batch should have identical invocation input messages
    assertTrue(client.wasSentToFunction.hasInvocation());
    assertThat(client.capturedInvocationBatchSize(), is(1));
    assertThat(client.capturedInvocation(0).getArgument(), is(argument));

    // re-sent batch should have new state as well as originally registered state
    assertThat(client.capturedStateNames().size(), is(2));
    assertThat(client.capturedStateNames(), hasItems("session", "new-state"));
  }

  @Test
  public void backlogMetricsIncreasedOnInvoke() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // following should be accounted into backlog metrics
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    assertThat(context.functionTypeMetrics().numBacklog, is(2));
  }

  @Test
  public void backlogMetricsDecreasedOnNextSuccess() {
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // following should be accounted into backlog metrics
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());
    functionUnderTest.invoke(context, TypedValue.getDefaultInstance());

    // complete one message, should fully consume backlog
    context.needsWaiting = false;
    functionUnderTest.invoke(context, successfulAsyncOperation());

    assertThat(context.functionTypeMetrics().numBacklog, is(0));
  }

  @Test
  public void retryBatchOnUnkownAsyncResponseAfterRestore() {
    TypedValue argument =
        TypedValue.newBuilder()
            .setTypename("io.statefun.foo/bar")
            .setValue(ByteString.copyFromUtf8("Hello!"))
            .build();
    functionUnderTest.invoke(context, argument);
    ToFunction originalRequest = client.wasSentToFunction;

    RequestReplyFunction restoredFunction =
        new RequestReplyFunction(FN_TYPE, new PersistedRemoteFunctionValues(), 2, client, true);
    restoredFunction.invoke(context, unknownAsyncOperation(originalRequest));

    // retry batch after a restore on an unknown async operation should start with empty state specs
    assertTrue(client.wasSentToFunction.hasInvocation());
    assertThat(client.capturedInvocationBatchSize(), is(1));
    assertThat(client.capturedInvocation(0).getArgument(), is(argument));
    assertThat(client.capturedStateNames().size(), is(0));
  }

  private static PersistedRemoteFunctionValues testInitialRegisteredState(
      String existingStateName, String typename) {
    final PersistedRemoteFunctionValues states = new PersistedRemoteFunctionValues();
    states.registerStates(
        Collections.singletonList(
            PersistedValueSpec.newBuilder()
                .setTypeTypename(typename)
                .setStateName(existingStateName)
                .build()));
    return states;
  }

  private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation() {
    return new AsyncOperationResult<>(
        new Object(), Status.SUCCESS, FromFunction.getDefaultInstance(), null);
  }

  private static AsyncOperationResult<Object, FromFunction> successfulAsyncOperation(
      FromFunction fromFunction) {
    return new AsyncOperationResult<>(new Object(), Status.SUCCESS, fromFunction, null);
  }

  private static AsyncOperationResult<ToFunction, FromFunction> successfulAsyncOperation(
      ToFunction toFunction, FromFunction fromFunction) {
    return new AsyncOperationResult<>(toFunction, Status.SUCCESS, fromFunction, null);
  }

  private static AsyncOperationResult<ToFunction, FromFunction> unknownAsyncOperation(
      ToFunction toFunction) {
    return new AsyncOperationResult<>(
        toFunction, Status.UNKNOWN, FromFunction.getDefaultInstance(), null);
  }

  private static final class FakeClient implements RequestReplyClient {
    ToFunction wasSentToFunction;
    Supplier<FromFunction> fromFunction = FromFunction::getDefaultInstance;

    @Override
    public CompletableFuture<FromFunction> call(
        ToFunctionRequestSummary requestSummary,
        RemoteInvocationMetrics metrics,
        ToFunction toFunction) {
      this.wasSentToFunction = toFunction;
      try {
        return CompletableFuture.completedFuture(this.fromFunction.get());
      } catch (Throwable t) {
        CompletableFuture<FromFunction> failed = new CompletableFuture<>();
        failed.completeExceptionally(t);
        return failed;
      }
    }

    /** return the n-th invocation sent as part of the current batch. */
    Invocation capturedInvocation(int n) {
      return wasSentToFunction.getInvocation().getInvocations(n);
    }

    TypedValue capturedState(int n) {
      return wasSentToFunction.getInvocation().getState(n).getStateValue();
    }

    Set<String> capturedStateNames() {
      return wasSentToFunction.getInvocation().getStateList().stream()
          .map(ToFunction.PersistedValue::getStateName)
          .collect(Collectors.toSet());
    }

    public int capturedInvocationBatchSize() {
      return wasSentToFunction.getInvocation().getInvocationsCount();
    }
  }

  private static final class DelayedMessage {
    final Duration delay;
    final @Nullable String cancellationToken;
    final Address target;
    final Object message;

    public DelayedMessage(
        Duration delay, @Nullable String cancellationToken, Address target, Object message) {
      this.delay = delay;
      this.cancellationToken = cancellationToken;
      this.target = target;
      this.message = message;
    }

    public Duration delay() {
      return delay;
    }

    @Nullable
    public String messageId() {
      return cancellationToken;
    }

    public Address target() {
      return target;
    }

    public Object message() {
      return message;
    }
  }

  private static final class FakeContext implements InternalContext {

    private final BacklogTrackingMetrics fakeMetrics = new BacklogTrackingMetrics();

    Address caller;
    boolean needsWaiting;

    // capture emitted messages
    List<Map.Entry<EgressIdentifier<?>, ?>> egresses = new ArrayList<>();
    List<DelayedMessage> delayed = new ArrayList<>();
    List<String> delayCancellations = new ArrayList<>();

    @Override
    public void awaitAsyncOperationComplete() {
      needsWaiting = true;
    }

    @Override
    public BacklogTrackingMetrics functionTypeMetrics() {
      return fakeMetrics;
    }

    @Override
    public Address self() {
      return new Address(FN_TYPE, "0");
    }

    @Override
    public Address caller() {
      return caller;
    }

    @Override
    public void send(Address to, Object message) {}

    @Override
    public <T> void send(EgressIdentifier<T> egress, T message) {
      egresses.add(new SimpleImmutableEntry<>(egress, message));
    }

    @Override
    public void sendAfter(Duration delay, Address to, Object message) {
      delayed.add(new DelayedMessage(delay, null, to, message));
    }

    @Override
    public void sendAfter(Duration delay, Address to, Object message, String cancellationToken) {
      delayed.add(new DelayedMessage(delay, cancellationToken, to, message));
    }

    @Override
    public void cancelDelayedMessage(String cancellationToken) {
      delayCancellations.add(cancellationToken);
    }

    @Override
    public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {}
  }

  private static final class BacklogTrackingMetrics implements FunctionTypeMetrics {

    private int numBacklog = 0;

    public int numBacklog() {
      return numBacklog;
    }

    @Override
    public void appendBacklogMessages(int count) {
      numBacklog += count;
    }

    @Override
    public void consumeBacklogMessages(int count) {
      numBacklog -= count;
    }

    @Override
    public void remoteInvocationFailures() {}

    @Override
    public void remoteInvocationLatency(long elapsed) {}

    @Override
    public void asyncOperationRegistered() {}

    @Override
    public void asyncOperationCompleted() {}

    @Override
    public void incomingMessage() {}

    @Override
    public void outgoingRemoteMessage() {}

    @Override
    public void outgoingEgressMessage() {}

    @Override
    public void outgoingLocalMessage() {}

    @Override
    public void blockedAddress() {}

    @Override
    public void unblockedAddress() {}
  }
}
