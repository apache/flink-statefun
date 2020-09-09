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

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public final class RequestReplyFunction implements StatefulFunction {

  private final RequestReplyClient client;
  private final int maxNumBatchRequests;

  /**
   * A request state keeps tracks of the number of inflight & batched requests.
   *
   * <p>A tracking state can have one of the following values:
   *
   * <ul>
   *   <li>NULL - there is no inflight request, and there is nothing in the backlog.
   *   <li>0 - there's an inflight request, but nothing in the backlog.
   *   <li>{@code > 0} There is an in flight request, and @requestState items in the backlog.
   * </ul>
   */
  @Persisted
  private final PersistedValue<Integer> requestState =
      PersistedValue.of("request-state", Integer.class);

  @Persisted
  private final PersistedAppendingBuffer<ToFunction.Invocation> batch =
      PersistedAppendingBuffer.of("batch", ToFunction.Invocation.class);

  @Persisted private final PersistedRemoteFunctionValues managedStates;

  public RequestReplyFunction(
      PersistedRemoteFunctionValues managedStates,
      int maxNumBatchRequests,
      RequestReplyClient client) {
    this.managedStates = Objects.requireNonNull(managedStates);
    this.client = Objects.requireNonNull(client);
    this.maxNumBatchRequests = maxNumBatchRequests;
  }

  @Override
  public void invoke(Context context, Object input) {
    InternalContext castedContext = (InternalContext) context;
    if (!(input instanceof AsyncOperationResult)) {
      onRequest(castedContext, (Any) input);
      return;
    }
    @SuppressWarnings("unchecked")
    AsyncOperationResult<ToFunction, FromFunction> result =
        (AsyncOperationResult<ToFunction, FromFunction>) input;
    onAsyncResult(castedContext, result);
  }

  private void onRequest(InternalContext context, Any message) {
    Invocation.Builder invocationBuilder = singeInvocationBuilder(context, message);
    int inflightOrBatched = requestState.getOrDefault(-1);
    if (inflightOrBatched < 0) {
      // no inflight requests, and nothing in the batch.
      // so we let this request to go through, and change state to indicate that:
      // a) there is a request in flight.
      // b) there is nothing in the batch.
      requestState.set(0);
      sendToFunction(context, invocationBuilder);
      return;
    }
    // there is at least one request in flight (inflightOrBatched >= 0),
    // so we add that request to the batch.
    batch.append(invocationBuilder.build());
    inflightOrBatched++;
    requestState.set(inflightOrBatched);
    context.functionTypeMetrics().appendBacklogMessages(1);
    if (isMaxNumBatchRequestsExceeded(inflightOrBatched)) {
      // we are at capacity, can't add anything to the batch.
      // we need to signal to the runtime that we are unable to process any new input
      // and we must wait for our in flight asynchronous operation to complete before
      // we are able to process more input.
      context.awaitAsyncOperationComplete();
    }
  }

  private void onAsyncResult(
      InternalContext context, AsyncOperationResult<ToFunction, FromFunction> asyncResult) {
    if (asyncResult.unknown()) {
      ToFunction batch = asyncResult.metadata();
      sendToFunction(context, batch);
      return;
    }
    InvocationResponse invocationResult = unpackInvocationOrThrow(context.self(), asyncResult);
    handleInvocationResponse(context, invocationResult);

    final int numBatched = requestState.getOrDefault(-1);
    if (numBatched < 0) {
      throw new IllegalStateException("Got an unexpected async result");
    } else if (numBatched == 0) {
      requestState.clear();
    } else {
      final InvocationBatchRequest.Builder nextBatch = getNextBatch();
      // an async request was just completed, but while it was in flight we have
      // accumulated a batch, we now proceed with:
      // a) clearing the batch from our own persisted state (the batch moves to the async operation
      // state)
      // b) sending the accumulated batch to the remote function.
      requestState.set(0);
      batch.clear();
      context.functionTypeMetrics().consumeBacklogMessages(numBatched);
      sendToFunction(context, nextBatch);
    }
  }

  private InvocationResponse unpackInvocationOrThrow(
      Address self, AsyncOperationResult<ToFunction, FromFunction> result) {
    if (result.failure()) {
      throw new IllegalStateException(
          "Failure forwarding a message to a remote function " + self, result.throwable());
    }
    FromFunction fromFunction = result.value();
    if (fromFunction.hasInvocationResult()) {
      return fromFunction.getInvocationResult();
    }
    return InvocationResponse.getDefaultInstance();
  }

  private InvocationBatchRequest.Builder getNextBatch() {
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    Iterable<Invocation> view = batch.view();
    builder.addAllInvocations(view);
    return builder;
  }

  private void handleInvocationResponse(Context context, InvocationResponse invocationResult) {
    handleOutgoingMessages(context, invocationResult);
    handleOutgoingDelayedMessages(context, invocationResult);
    handleEgressMessages(context, invocationResult);
    handleStateMutations(invocationResult);
  }

  private void handleEgressMessages(Context context, InvocationResponse invocationResult) {
    for (EgressMessage egressMessage : invocationResult.getOutgoingEgressesList()) {
      EgressIdentifier<Any> id =
          new EgressIdentifier<>(
              egressMessage.getEgressNamespace(), egressMessage.getEgressType(), Any.class);
      context.send(id, egressMessage.getArgument());
    }
  }

  private void handleOutgoingMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.Invocation invokeCommand : invocationResult.getOutgoingMessagesList()) {
      final Address to = polyglotAddressToSdkAddress(invokeCommand.getTarget());
      final Any message = invokeCommand.getArgument();

      context.send(to, message);
    }
  }

  private void handleOutgoingDelayedMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.DelayedInvocation delayedInvokeCommand :
        invocationResult.getDelayedInvocationsList()) {
      final Address to = polyglotAddressToSdkAddress(delayedInvokeCommand.getTarget());
      final Any message = delayedInvokeCommand.getArgument();
      final long delay = delayedInvokeCommand.getDelayInMs();

      context.sendAfter(Duration.ofMillis(delay), to, message);
    }
  }

  // --------------------------------------------------------------------------------
  // State Management
  // --------------------------------------------------------------------------------

  private void addStates(ToFunction.InvocationBatchRequest.Builder batchBuilder) {
    managedStates.forEach(
        (stateName, stateValue) -> {
          ToFunction.PersistedValue.Builder valueBuilder =
              ToFunction.PersistedValue.newBuilder().setStateName(stateName);

          if (stateValue != null) {
            valueBuilder.setStateValue(ByteString.copyFrom(stateValue));
          }
          batchBuilder.addState(valueBuilder);
        });
  }

  private void handleStateMutations(InvocationResponse invocationResult) {
    for (FromFunction.PersistedValueMutation mutate : invocationResult.getStateMutationsList()) {
      final String stateName = mutate.getStateName();
      switch (mutate.getMutationType()) {
        case DELETE:
          managedStates.clearValue(stateName);
          break;
        case MODIFY:
          managedStates.setValue(stateName, mutate.getStateValue().toByteArray());
          break;
        case UNRECOGNIZED:
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + mutate.getMutationType());
      }
    }
  }

  // --------------------------------------------------------------------------------
  // Send Message to Remote Function
  // --------------------------------------------------------------------------------
  /**
   * Returns an {@link Invocation.Builder} set with the input {@code message} and the caller
   * information (is present).
   */
  private static Invocation.Builder singeInvocationBuilder(Context context, Any message) {
    Invocation.Builder invocationBuilder = Invocation.newBuilder();
    if (context.caller() != null) {
      invocationBuilder.setCaller(sdkAddressToPolyglotAddress(context.caller()));
    }
    invocationBuilder.setArgument(message);
    return invocationBuilder;
  }

  /**
   * Sends a {@link InvocationBatchRequest} to the remote function consisting out of a single
   * invocation represented by {@code invocationBuilder}.
   */
  private void sendToFunction(Context context, Invocation.Builder invocationBuilder) {
    InvocationBatchRequest.Builder batchBuilder = InvocationBatchRequest.newBuilder();
    batchBuilder.addInvocations(invocationBuilder);
    sendToFunction(context, batchBuilder);
  }

  /** Sends a {@link InvocationBatchRequest} to the remote function. */
  private void sendToFunction(Context context, InvocationBatchRequest.Builder batchBuilder) {
    batchBuilder.setTarget(sdkAddressToPolyglotAddress(context.self()));
    addStates(batchBuilder);
    ToFunction toFunction = ToFunction.newBuilder().setInvocation(batchBuilder).build();
    sendToFunction(context, toFunction);
  }

  private void sendToFunction(Context context, ToFunction toFunction) {
    ToFunctionRequestSummary requestSummary =
        new ToFunctionRequestSummary(
            context.self(),
            toFunction.getSerializedSize(),
            toFunction.getInvocation().getStateCount(),
            toFunction.getInvocation().getInvocationsCount());
    RemoteInvocationMetrics metrics = ((InternalContext) context).functionTypeMetrics();
    CompletableFuture<FromFunction> responseFuture =
        client.call(requestSummary, metrics, toFunction);
    context.registerAsyncOperation(toFunction, responseFuture);
  }

  private boolean isMaxNumBatchRequestsExceeded(final int currentNumBatchRequests) {
    return maxNumBatchRequests > 0 && currentNumBatchRequests >= maxNumBatchRequests;
  }
}
