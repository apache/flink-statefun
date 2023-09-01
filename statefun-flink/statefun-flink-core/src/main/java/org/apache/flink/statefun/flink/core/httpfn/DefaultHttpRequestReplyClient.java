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
import static org.apache.flink.util.Preconditions.checkState;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.util.IOUtils;

final class DefaultHttpRequestReplyClient implements RequestReplyClient {
  private static final MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/octet-stream");

  private final HttpUrl url;
  private final OkHttpClient client;
  private final BooleanSupplier isShutdown;

  DefaultHttpRequestReplyClient(HttpUrl url, OkHttpClient client, BooleanSupplier isShutdown) {
    this.url = Objects.requireNonNull(url);
    this.client = Objects.requireNonNull(client);
    this.isShutdown = Objects.requireNonNull(isShutdown);
  }

  @Override
  public CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction,
      int maxRetries) {
    Request request =
        new Request.Builder()
            .url(url)
            .post(RequestBody.create(MEDIA_TYPE_BINARY, toFunction.toByteArray()))
            .build();

    Call newCall = client.newCall(request);
    RetryingCallback callback =
        new RetryingCallback(requestSummary, metrics, newCall.timeout(), isShutdown, maxRetries);
    callback.attachToCall(newCall);
    return callback.future().thenApply(DefaultHttpRequestReplyClient::parseResponse);
  }

  private static FromFunction parseResponse(Response response) {
    if (response == null) {
      return null;
    }

final InputStream httpResponseBody = responseBody(response);
    try {
      return parseProtobufOrThrow(FromFunction.parser(), httpResponseBody);
    } finally {
      IOUtils.closeQuietly(httpResponseBody);
    }
  }

  private static InputStream responseBody(Response httpResponse) {
    checkState(httpResponse.isSuccessful(), "Unexpected HTTP status code %s", httpResponse.code());
    checkState(httpResponse.body() != null, "Unexpected empty HTTP response (no body)");
    checkState(
        Objects.equals(httpResponse.body().contentType(), MEDIA_TYPE_BINARY),
        "Wrong HTTP content-type %s",
        httpResponse.body().contentType());
    return httpResponse.body().byteStream();
  }
}
