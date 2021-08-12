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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.statefun.flink.core.backpressure.InternalContext;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.EgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.IncompleteInvocationContext;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.Invocation;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.types.Either;

public final class RequestReplyFunction implements StatefulFunction {

  private final RequestReplyClient client;
  private final int maxNumBatchRequests;

  /**
   * This flag indicates whether or not at least one request has already been sent to the remote
   * function. It is toggled by the {@link #sendToFunction(InternalContext, ToFunction)} method upon
   * sending the first request.
   *
   * <p>For the first request, we block until response is received; for stateful applications,
   * especially at restore time of a restored execution where there may be a large backlog of events
   * and checkpointed inflight requests, this helps mitigate excessive hoards of
   * IncompleteInvocationContext responses and retry attempt round-trips.
   *
   * <p>After this flag is toggled upon sending the first request, all successive requests will be
   * performed as usual async operations.
   */
  private boolean isFirstRequestSent = false;

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

  public RequestReplyFunction(int maxNumBatchRequests, RequestReplyClient client) {
    this(new PersistedRemoteFunctionValues(), maxNumBatchRequests, client);
  }

  @VisibleForTesting
  RequestReplyFunction(
      PersistedRemoteFunctionValues states, int maxNumBatchRequests, RequestReplyClient client) {
    this.managedStates = Objects.requireNonNull(states);
    this.maxNumBatchRequests = maxNumBatchRequests;
    this.client = Objects.requireNonNull(client);
  }

  @Override
  public void invoke(Context context, Object input) {
    InternalContext castedContext = (InternalContext) context;
    if (!(input instanceof AsyncOperationResult)) {
      onRequest(castedContext, (TypedValue) input);
      return;
    }
    @SuppressWarnings("unchecked")
    AsyncOperationResult<ToFunction, FromFunction> result =
        (AsyncOperationResult<ToFunction, FromFunction>) input;
    onAsyncResult(castedContext, result);
  }

  private void onRequest(InternalContext context, TypedValue message) {
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
      // According to the request-reply protocol, on recovery
      // we re-send the uncompleted (in-flight during a failure)
      // messages. But we shouldn't assume that the state
      // definitions are the same as in the previous attempt.
      // We create a retry batch and let the SDK reply
      // with the correct state specs.
      sendToFunction(context, createRetryBatch(batch));
      return;
    }
    if (asyncResult.failure()) {
      throw new IllegalStateException(
          "Failure forwarding a message to a remote function " + context.self(),
          asyncResult.throwable());
    }

    final Either<InvocationResponse, IncompleteInvocationContext> response =
        unpackResponse(asyncResult.value());
    if (response.isRight()) {
      handleIncompleteInvocationContextResponse(context, response.right(), asyncResult.metadata());
    } else {
      handleInvocationResultResponse(context, response.left());
    }
  }

  private static Either<InvocationResponse, IncompleteInvocationContext> unpackResponse(
      FromFunction fromFunction) {
    if (fromFunction.hasIncompleteInvocationContext()) {
      return Either.Right(fromFunction.getIncompleteInvocationContext());
    }
    if (fromFunction.hasInvocationResult()) {
      return Either.Left(fromFunction.getInvocationResult());
    }
    // function had no side effects
    return Either.Left(InvocationResponse.getDefaultInstance());
  }

  private void handleIncompleteInvocationContextResponse(
      InternalContext context,
      IncompleteInvocationContext incompleteContext,
      ToFunction originalBatch) {
    managedStates.registerStates(incompleteContext.getMissingValuesList());

    final InvocationBatchRequest.Builder retryBatch = createRetryBatch(originalBatch);
    sendToFunction(context, retryBatch);
  }

  private void handleInvocationResultResponse(InternalContext context, InvocationResponse result) {
    handleOutgoingMessages(context, result);
    handleOutgoingDelayedMessages(context, result);
    handleEgressMessages(context, result);
    managedStates.updateStateValues(result.getStateMutationsList());

    final int numBatched = requestState.getOrDefault(-1);
    if (numBatched < 0) {
      throw new IllegalStateException("Got an unexpected async result");
    } else if (numBatched == 0) {
      requestState.clear();
    } else {
      final InvocationBatchRequest.Builder nextBatch = getNextBatch();
      // an async request was just completed, but while it was in flight we have
      // accumulated a batch, we now proceed with:
      // a) clearing the batch from our own persisted state (the batch moves to the async
      // operation
      // state)
      // b) sending the accumulated batch to the remote function.
      requestState.set(0);
      batch.clear();
      context.functionTypeMetrics().consumeBacklogMessages(numBatched);
      sendToFunction(context, nextBatch);
    }
  }

  private InvocationBatchRequest.Builder getNextBatch() {
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    Iterable<Invocation> view = batch.view();
    builder.addAllInvocations(view);
    return builder;
  }

  private InvocationBatchRequest.Builder createRetryBatch(ToFunction toFunction) {
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    builder.addAllInvocations(toFunction.getInvocation().getInvocationsList());
    return builder;
  }

  private void handleEgressMessages(Context context, InvocationResponse invocationResult) {
    for (EgressMessage egressMessage : invocationResult.getOutgoingEgressesList()) {
      EgressIdentifier<TypedValue> id =
          new EgressIdentifier<>(
              egressMessage.getEgressNamespace(), egressMessage.getEgressType(), TypedValue.class);
      context.send(id, egressMessage.getArgument());
    }
  }

  private void handleOutgoingMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.Invocation invokeCommand : invocationResult.getOutgoingMessagesList()) {
      final Address to = polyglotAddressToSdkAddress(invokeCommand.getTarget());
      final TypedValue message = invokeCommand.getArgument();

      context.send(to, message);
    }
  }

  private void handleOutgoingDelayedMessages(Context context, InvocationResponse invocationResult) {
    for (FromFunction.DelayedInvocation delayedInvokeCommand :
        invocationResult.getDelayedInvocationsList()) {

      if (delayedInvokeCommand.getIsCancellationRequest()) {
        handleDelayedMessageCancellation(context, delayedInvokeCommand);
      } else {
        handleDelayedMessageSending(context, delayedInvokeCommand);
      }
    }
  }

  private void handleDelayedMessageSending(
      Context context, FromFunction.DelayedInvocation delayedInvokeCommand) {
    final Address to = polyglotAddressToSdkAddress(delayedInvokeCommand.getTarget());
    final TypedValue message = delayedInvokeCommand.getArgument();
    final long delay = delayedInvokeCommand.getDelayInMs();

    context.sendAfter(Duration.ofMillis(delay), to, message);
  }

  private void handleDelayedMessageCancellation(
      Context context, FromFunction.DelayedInvocation delayedInvokeCommand) {
    String token = delayedInvokeCommand.getCancellationToken();
    if (token.isEmpty()) {
      throw new IllegalArgumentException(
          "Can not handle a cancellation request without a cancellation token.");
    }
    context.cancelDelayedMessage(token);
  }

  // --------------------------------------------------------------------------------
  // Send Message to Remote Function
  // --------------------------------------------------------------------------------
  /**
   * Returns an {@link Invocation.Builder} set with the input {@code message} and the caller
   * information (is present).
   */
  private static Invocation.Builder singeInvocationBuilder(Context context, TypedValue message) {
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
  private void sendToFunction(InternalContext context, Invocation.Builder invocationBuilder) {
    InvocationBatchRequest.Builder batchBuilder = InvocationBatchRequest.newBuilder();
    batchBuilder.addInvocations(invocationBuilder);
    sendToFunction(context, batchBuilder);
  }

  /** Sends a {@link InvocationBatchRequest} to the remote function. */
  private void sendToFunction(
      InternalContext context, InvocationBatchRequest.Builder batchBuilder) {
    batchBuilder.setTarget(sdkAddressToPolyglotAddress(context.self()));
    managedStates.attachStateValues(batchBuilder);
    ToFunction toFunction = ToFunction.newBuilder().setInvocation(batchBuilder).build();
    sendToFunction(context, toFunction);
  }

  private void sendToFunction(InternalContext context, ToFunction toFunction) {
    ToFunctionRequestSummary requestSummary =
        new ToFunctionRequestSummary(
            context.self(),
            toFunction.getSerializedSize(),
            toFunction.getInvocation().getStateCount(),
            toFunction.getInvocation().getInvocationsCount());
    RemoteInvocationMetrics metrics = context.functionTypeMetrics();
    CompletableFuture<FromFunction> responseFuture =
        client.call(requestSummary, metrics, toFunction);

    if (isFirstRequestSent) {
      context.registerAsyncOperation(toFunction, responseFuture);
    } else {
      // it is important to toggle the flag *before* handling the response. As a result of handling
      // the first response, we may send retry requests in the case of an
      // IncompleteInvocationContext response. For those requests, we already want to handle them as
      // usual async operations.
      isFirstRequestSent = true;
      onAsyncResult(context, joinResponse(responseFuture, toFunction));
    }
  }

  private boolean isMaxNumBatchRequestsExceeded(final int currentNumBatchRequests) {
    return maxNumBatchRequests > 0 && currentNumBatchRequests >= maxNumBatchRequests;
  }

  private AsyncOperationResult<ToFunction, FromFunction> joinResponse(
      CompletableFuture<FromFunction> responseFuture, ToFunction originalRequest) {
    FromFunction response;
    try {
      response = responseFuture.join();
    } catch (Exception e) {
      return new AsyncOperationResult<>(
          originalRequest, AsyncOperationResult.Status.FAILURE, null, e.getCause());
    }
    return new AsyncOperationResult<>(
        originalRequest, AsyncOperationResult.Status.SUCCESS, response, null);
  }
}
