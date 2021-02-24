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

import static org.apache.flink.statefun.sdk.java.handler.MoreFutures.applySequentially;
import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.sdkAddressFromProto;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.message.MessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.java.storage.StateValueContexts;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

/**
 * A threadsafe {@linkplain RequestReplyHandler}. This handler lifecycle is bound to the entire
 * program, and can be safely and concurrently used to handle {@linkplain ToFunction} requests.
 */
@Internal
public final class ConcurrentRequestReplyHandler implements RequestReplyHandler {
  private final Map<TypeName, StatefulFunctionSpec> functionSpecs;

  public ConcurrentRequestReplyHandler(Map<TypeName, StatefulFunctionSpec> functionSpecs) {
    this.functionSpecs = Objects.requireNonNull(functionSpecs);
  }

  @Override
  public CompletableFuture<Slice> handle(Slice requestBytes) {
    try {
      ByteString in = SliceProtobufUtil.asByteString(requestBytes);
      ToFunction request = ToFunction.parseFrom(in);
      CompletableFuture<FromFunction> response = handleInternally(request);
      return response.thenApply(
          res -> {
            ByteString out = res.toByteString();
            return SliceProtobufUtil.asSlice(out);
          });
    } catch (Throwable throwable) {
      return MoreFutures.exceptional(throwable);
    }
  }

  CompletableFuture<FromFunction> handleInternally(ToFunction request) {
    if (!request.hasInvocation()) {
      return CompletableFuture.completedFuture(FromFunction.getDefaultInstance());
    }
    ToFunction.InvocationBatchRequest batchRequest = request.getInvocation();
    Address self = sdkAddressFromProto(batchRequest.getTarget());
    StatefulFunctionSpec targetSpec = functionSpecs.get(self.type());
    if (targetSpec == null) {
      throw new IllegalStateException("Unknown target type " + self);
    }
    Supplier<? extends StatefulFunction> supplier = targetSpec.supplier();
    if (supplier == null) {
      throw new NullPointerException("missing function supplier for " + self);
    }
    StatefulFunction function = supplier.get();
    if (function == null) {
      throw new NullPointerException("supplier for " + self + " supplied NULL function.");
    }
    StateValueContexts.ResolutionResult stateResolution =
        StateValueContexts.resolve(targetSpec.knownValues(), batchRequest.getStateList());
    if (stateResolution.hasMissingValues()) {
      // not enough information to compute this batch.
      FromFunction res = buildIncompleteInvocationResponse(stateResolution.missingValues());
      return CompletableFuture.completedFuture(res);
    }
    final ConcurrentAddressScopedStorage storage =
        new ConcurrentAddressScopedStorage(stateResolution.resolved());
    return executeBatch(batchRequest, self, storage, function);
  }

  private CompletableFuture<FromFunction> executeBatch(
      ToFunction.InvocationBatchRequest inputBatch,
      Address self,
      ConcurrentAddressScopedStorage storage,
      StatefulFunction function) {

    FromFunction.InvocationResponse.Builder responseBuilder =
        FromFunction.InvocationResponse.newBuilder();

    ConcurrentContext context = new ConcurrentContext(self, responseBuilder, storage);

    CompletableFuture<Void> allDone =
        applySequentially(
            inputBatch.getInvocationsList(), invocation -> apply(function, context, invocation));

    return allDone.thenApply(unused -> finalizeResponse(storage, context.finalBuilder()));
  }

  private static FromFunction buildIncompleteInvocationResponse(List<ValueSpec<?>> missing) {
    FromFunction.IncompleteInvocationContext.Builder result =
        FromFunction.IncompleteInvocationContext.newBuilder();

    for (ValueSpec<?> v : missing) {
      result.addMissingValues(ProtoUtils.protoFromValueSpec(v));
    }

    return FromFunction.newBuilder().setIncompleteInvocationContext(result).build();
  }

  private static CompletableFuture<Void> apply(
      StatefulFunction function, ConcurrentContext context, ToFunction.Invocation invocation)
      throws Throwable {
    TypedValue argument = invocation.getArgument();
    MessageWrapper wrapper = new MessageWrapper(context.self(), argument);
    context.setCaller(sdkAddressFromProto(invocation.getCaller()));
    CompletableFuture<Void> future = function.apply(context, wrapper);
    if (future == null) {
      throw new IllegalStateException(
          "User function " + context.self() + " has returned a NULL future.");
    }
    return future;
  }

  private static FromFunction finalizeResponse(
      ConcurrentAddressScopedStorage storage, FromFunction.InvocationResponse.Builder builder) {
    storage.addMutations(builder::addStateMutations);
    return FromFunction.newBuilder().setInvocationResult(builder).build();
  }
}
