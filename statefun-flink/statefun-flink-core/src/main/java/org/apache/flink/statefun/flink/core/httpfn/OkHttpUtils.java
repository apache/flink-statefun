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

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

final class OkHttpUtils {
  private OkHttpUtils() {}

  public static CompletableFuture<Response> call(OkHttpClient client, Request request) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    client.newCall(request).enqueue(new CompletableFutureCallback(future));
    return future;
  }

  @SuppressWarnings("NullableProblems")
  private static final class CompletableFutureCallback implements Callback {
    private final CompletableFuture<Response> future;

    public CompletableFutureCallback(CompletableFuture<Response> future) {
      this.future = Objects.requireNonNull(future);
    }

    @Override
    public void onFailure(Call call, IOException e) {
      future.completeExceptionally(e);
    }

    @Override
    public void onResponse(Call call, Response response) {
      future.complete(response);
    }
  }
}
