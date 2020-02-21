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

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.parseProtobufOrThrow;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.polyglotAddressToSdkAddress;
import static org.apache.flink.statefun.flink.core.common.PolyglotUtil.sdkAddressToPolyglotAddress;
import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUtils.MEDIA_TYPE_BINARY;
import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUtils.responseBody;
import static org.apache.flink.util.Preconditions.checkState;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flink.statefun.flink.core.backpressure.AsyncWaiter;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.util.IOUtils;

final class HttpFunction implements StatefulFunction {

  private final HttpFunctionSpec functionSpec;
  private final OkHttpClient client;
  private final HttpUrl url;

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

  @Persisted
  private final PersistedTable<String, byte[]> managedStates =
      PersistedTable.of("states", String.class, byte[].class);

  public HttpFunction(HttpFunctionSpec spec, OkHttpClient client) {
    this.functionSpec = Objects.requireNonNull(spec);
    this.client = Objects.requireNonNull(client);
    this.url = HttpUrl.get(functionSpec.endpoint());
  }

  @Override
  public void invoke(Context context, Object input) {
    if (!(input instanceof AsyncOperationResult)) {
      onRequest(context, (Any) input);
      return;
    }
    @SuppressWarnings("unchecked")
    AsyncOperationResult<ToFunction, Response> result =
        (AsyncOperationResult<ToFunction, Response>) input;
    onAsyncResult(context, result);
  }

  private void onRequest(Context context, Any message) {
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
    if (isMaxBatchSizeExceeded(inflightOrBatched)) {
      // we are at capacity, can't add anything to the batch.
      // we need to signal to the runtime that we are unable to process any new input
      // and we must wait for our in flight asynchronous operation to complete before
      // we are able to process more input.
      ((AsyncWaiter) context).awaitAsyncOperationComplete();
    }
  }

  private void onAsyncResult(
      Context context, AsyncOperationResult<ToFunction, Response> asyncResult) {
    if (asyncResult.unknown()) {
      ToFunction batch = asyncResult.metadata();
      sendToFunction(context, batch);
      return;
    }
    InvocationResponse invocationResult =
        unpackInvocationResultOrThrow(context.self(), asyncResult);
    handleInvocationResponse(context, invocationResult);
    InvocationBatchRequest.Builder nextBatch = getNextBatch();
    if (nextBatch == null) {
      // the async request was completed, and there is nothing else in the batch
      // so we clear the requestState.
      requestState.clear();
      return;
    }
    // an async request was just completed, but while it was in flight we have
    // accumulated a batch, we now proceed with:
    // a) clearing the batch from our own persisted state (the batch moves to the async operation
    // state)
    // b) sending the accumulated batch to the remote function.
    requestState.set(0);
    batch.clear();
    sendToFunction(context, nextBatch);
  }

  @Nullable
  private InvocationBatchRequest.Builder getNextBatch() {
    @Nullable Iterable<Invocation> next = batch.view();
    if (next == null) {
      return null;
    }
    InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    builder.addAllInvocations(next);
    return builder;
  }

  private void handleInvocationResponse(Context context, InvocationResponse invocationResult) {
    for (FromFunction.Invocation invokeCommand : invocationResult.getOutgoingMessagesList()) {
      final org.apache.flink.statefun.sdk.Address to =
          polyglotAddressToSdkAddress(invokeCommand.getTarget());
      final Any message = invokeCommand.getArgument();

      context.send(to, message);
    }
    handleStateMutations(invocationResult);
  }

  // --------------------------------------------------------------------------------
  // State Management
  // --------------------------------------------------------------------------------

  private void addStates(ToFunction.InvocationBatchRequest.Builder batchBuilder) {
    for (String stateName : functionSpec.states()) {
      ToFunction.PersistedValue.Builder valueBuilder =
          ToFunction.PersistedValue.newBuilder().setStateName(stateName);

      byte[] stateValue = managedStates.get(stateName);
      if (stateValue != null) {
        valueBuilder.setStateValue(ByteString.copyFrom(stateValue));
      }
      batchBuilder.addState(valueBuilder);
    }
  }

  private void handleStateMutations(InvocationResponse invocationResult) {
    for (FromFunction.PersistedValueMutation mutate : invocationResult.getStateMutationsList()) {
      final String stateName = mutate.getStateName();
      switch (mutate.getMutationType()) {
        case DELETE:
          managedStates.remove(stateName);
          break;
        case MODIFY:
          managedStates.set(stateName, mutate.getStateValue().toByteArray());
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
    Request request =
        new Request.Builder()
            .url(url)
            .post(RequestBody.create(MEDIA_TYPE_BINARY, toFunction.toByteArray()))
            .build();

    CompletableFuture<Response> responseFuture = OkHttpUtils.call(client, request);
    context.registerAsyncOperation(toFunction, responseFuture);
  }

  private InvocationResponse unpackInvocationResultOrThrow(
      Address self, AsyncOperationResult<?, Response> asyncResult) {
    checkState(!asyncResult.unknown());
    if (asyncResult.failure()) {
      throw new IllegalStateException(
          "Failure forwarding a message to a remote function " + self, asyncResult.throwable());
    }
    InputStream httpResponseBody = responseBody(asyncResult.value());
    try {
      FromFunction fromFunction = parseProtobufOrThrow(FromFunction.parser(), httpResponseBody);
      if (fromFunction.hasInvocationResult()) {
        return fromFunction.getInvocationResult();
      }
      return InvocationResponse.getDefaultInstance();
    } finally {
      IOUtils.closeQuietly(httpResponseBody);
    }
  }

  private boolean isMaxBatchSizeExceeded(final int currentBatchSize) {
    int maxBatchSize = functionSpec.maxBatchSize();
    return maxBatchSize > 0 && currentBatchSize >= maxBatchSize;
  }
}
