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
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
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
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class HttpFunction implements StatefulFunction {

  private final HttpFunctionSpec functionSpec;
  private final OkHttpClient client;
  private final HttpUrl url;

  @Persisted
  private final PersistedValue<Boolean> hasInFlightRpc =
      PersistedValue.of("inflight", Boolean.class);

  @Persisted
  private final PersistedValue<ToFunction.InvocationBatchRequest> batch =
      PersistedValue.of("batch", ToFunction.InvocationBatchRequest.class);

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
    if (hasInFlightRpc.getOrDefault(Boolean.FALSE)) {
      addToOrCreateBatch(invocationBuilder);
      return;
    }
    hasInFlightRpc.set(Boolean.TRUE);
    sendToFunction(context, invocationBuilder);
  }

  private void addToOrCreateBatch(Invocation.Builder invocationBuilder) {
    InvocationBatchRequest current = batch.get();
    InvocationBatchRequest.Builder next =
        (current == null) ? InvocationBatchRequest.newBuilder() : current.toBuilder();
    next.addInvocations(invocationBuilder);
    batch.set(next.build());
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
    InvocationBatchRequest nextBatch = batch.get();
    if (nextBatch == null) {
      hasInFlightRpc.clear();
      return;
    }
    batch.clear();
    sendToFunction(context, nextBatch.toBuilder());
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
    FromFunction fromFunction = parseProtobufOrThrow(FromFunction.parser(), httpResponseBody);
    checkState(
        fromFunction.hasInvocationResult(),
        "The received HTTP payload does not contain an InvocationResult, but rather [%s]",
        fromFunction);
    return fromFunction.getInvocationResult();
  }
}
