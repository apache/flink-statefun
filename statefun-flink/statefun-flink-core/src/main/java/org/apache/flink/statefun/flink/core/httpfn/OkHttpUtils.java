/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.util.Preconditions.checkState;

import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

final class OkHttpUtils {
  private OkHttpUtils() {}

  private static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofMinutes(2);

  static final MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/octet-stream");

  static OkHttpClient newClient() {
    Dispatcher dispatcher = new Dispatcher();
    dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
    dispatcher.setMaxRequests(Integer.MAX_VALUE);

    return new OkHttpClient.Builder()
        .callTimeout(DEFAULT_CALL_TIMEOUT)
        .dispatcher(dispatcher)
        .connectionPool(new ConnectionPool())
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(true)
        .build();
  }

  static CompletableFuture<Response> call(OkHttpClient client, Request request) {
    Call newCall = client.newCall(request);
    RetryingCallback callback = new RetryingCallback(newCall.timeout());
    newCall.enqueue(callback);
    return callback.future();
  }

  static InputStream responseBody(Response httpResponse) {
    checkState(httpResponse.isSuccessful(), "Unexpected HTTP status code %s", httpResponse.code());
    checkState(httpResponse.body() != null, "Unexpected empty HTTP response (no body)");
    checkState(
        Objects.equals(httpResponse.body().contentType(), MEDIA_TYPE_BINARY),
        "Wrong HTTP content-type %s",
        httpResponse.body().contentType());
    return httpResponse.body().byteStream();
  }
}
