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

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OkHttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OkHttpUtils.class);

  private OkHttpUtils() {}

  static OkHttpClient newClient() {
    Dispatcher dispatcher = new Dispatcher();
    dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
    dispatcher.setMaxRequests(Integer.MAX_VALUE);

    ConnectionPool connectionPool = new ConnectionPool(1024, 1, TimeUnit.MINUTES);

    return new OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .connectionPool(connectionPool)
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(true)
        .build();
  }

  static void closeSilently(@Nullable OkHttpClient client) {
    if (client == null) {
      return;
    }
    final Dispatcher dispatcher = client.dispatcher();
    try {
      dispatcher.executorService().shutdownNow();
    } catch (Throwable ignored) {
    }
    try {
      dispatcher.cancelAll();
    } catch (Throwable throwable) {
      LOG.warn("Exception caught while trying to close the HTTP client", throwable);
    }
    try {
      client.connectionPool().evictAll();
    } catch (Throwable throwable) {
      LOG.warn("Exception caught while trying to close the HTTP connection pool", throwable);
    }
  }
}
