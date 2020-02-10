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

import org.apache.flink.statefun.flink.core.polyglot.generated.Address;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.InvocationResponse;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.Invocation.Builder;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkState;

final class HttpFunction implements StatefulFunction {

  private static final MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/octet-stream");

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
    if (input instanceof AsyncOperationResult) {
      @SuppressWarnings("unchecked")
      AsyncOperationResult<Any, Response> result = (AsyncOperationResult<Any, Response>) input;
      onAsyncResult(context, result);
    } else {
      onRequest(context, (Any) input);
    }
  }

  private void onRequest(Context context, Any input) {
    Invocation.Builder invocationBuilder = invocationBuilder(context, input);
    if (hasInFlightRpc.getOrDefault(Boolean.FALSE)) {
      addToOrCreateBatch(invocationBuilder);
      return;
    }
    hasInFlightRpc.set(Boolean.TRUE);
    sendToFunction(context, invocationBuilder);
  }

  private void onAsyncResult(Context context, AsyncOperationResult<Any, Response> asyncResult) {
    if (asyncResult.unknown()) {
      Any originalMessage = asyncResult.metadata();
      Invocation.Builder invocationBuilder = invocationBuilder(context, originalMessage);
      sendToFunction(context, invocationBuilder);
      return;
    }
    InvocationResponse invocationResult = unpackInvocationResultOrThrow(asyncResult);
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

  private void addToOrCreateBatch(Builder invocationBuilder) {
    batch.updateAndGet(
        existingBatch -> {
          final InvocationBatchRequest.Builder builder =
              existingBatch != null
                  ? existingBatch.toBuilder()
                  : InvocationBatchRequest.newBuilder();
          return builder.addInvocations(invocationBuilder).build();
        });
  }

  // --------------------------------------------------------------------------------
  // Utilities
  // --------------------------------------------------------------------------------
  private static Builder invocationBuilder(Context context, Any input) {
    Invocation.Builder invocationBuilder = Invocation.newBuilder();
    if (context.caller() != null) {
      invocationBuilder.setCaller(sdkAddressToPolyglotAddress(context.caller()));
    }
    invocationBuilder.setArgument(input);
    return invocationBuilder;
  }

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

  private void sendToFunction(Context context, Invocation.Builder invocationBuilder) {
    InvocationBatchRequest.Builder batchBuilder = InvocationBatchRequest.newBuilder();
    batchBuilder.addInvocations(invocationBuilder);
    sendToFunction(context, batchBuilder);
  }

  private void sendToFunction(Context context, InvocationBatchRequest.Builder batchBuilder) {
    batchBuilder.setTarget(sdkAddressToPolyglotAddress(context.self()));
    addStates(batchBuilder);
    Request request =
        new Request.Builder()
            .url(url)
            .post(
                RequestBody.create(
                    MEDIA_TYPE_BINARY,
                    ToFunction.newBuilder().setInvocation(batchBuilder).build().toByteArray()))
            .build();
    OkHttpUtils.call(client, request);
  }

  private static InvocationResponse unpackInvocationResultOrThrow(
      AsyncOperationResult<Any, Response> asyncResult) {
    checkState(asyncResult.failure() || asyncResult.successful());
    if (asyncResult.failure()) {
      throw new IllegalStateException("", asyncResult.throwable());
    }
    Response httpResponse = asyncResult.value();
    checkState(httpResponse.isSuccessful(), "Unexpected HTTP status code %s", httpResponse.code());
    checkState(httpResponse.body() != null, "Unexpected empty HTTP response (no body)");
    checkState(
        Objects.equals(httpResponse.body().contentType(), MEDIA_TYPE_BINARY),
        "Wrong HTTP content-type %s",
        httpResponse.body().contentType());
    InputStream httpResponseBody = httpResponse.body().byteStream();
    FromFunction fromFunction = parseProtobufOrThrow(httpResponseBody);
    checkState(
        fromFunction.hasInvocationResult(),
        "The received HTTP payload does not contain an InvocationResult, but rather [%s]",
        fromFunction);
    return fromFunction.getInvocationResult();
  }

  private static FromFunction parseProtobufOrThrow(InputStream input) {
    try {
      return FromFunction.parseFrom(input);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse a Protobuf message", e);
    }
  }

  private static Address sdkAddressToPolyglotAddress(
      @Nonnull org.apache.flink.statefun.sdk.Address sdkAddress) {
    return Address.newBuilder()
        .setNamespace(sdkAddress.type().namespace())
        .setType(sdkAddress.type().name())
        .setId(sdkAddress.id())
        .build();
  }

  private static org.apache.flink.statefun.sdk.Address polyglotAddressToSdkAddress(
      Address address) {
    return new org.apache.flink.statefun.sdk.Address(
        new FunctionType(address.getNamespace(), address.getType()), address.getId());
  }
}
