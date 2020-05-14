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

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.newsclub.net.unix.AFUNIXSocketFactory;

public class HttpFunctionProvider implements StatefulFunctionProvider {
  private final Map<FunctionType, HttpFunctionSpec> supportedTypes;
  private final OkHttpClient sharedClient;

  public HttpFunctionProvider(Map<FunctionType, HttpFunctionSpec> supportedTypes) {
    this.supportedTypes = supportedTypes;
    this.sharedClient = OkHttpUtils.newClient();
  }

  @Override
  public RequestReplyFunction functionOfType(FunctionType type) {
    HttpFunctionSpec spec = supportedTypes.get(type);
    if (spec == null) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    return new RequestReplyFunction(
        spec.states(), spec.maxNumBatchRequests(), buildHttpClient(spec));
  }

  private RequestReplyClient buildHttpClient(HttpFunctionSpec spec) {
    // We need to build a UDS HTTP client
    if (spec.unixDomainSocket() != null) {
      OkHttpClient specificClient =
          sharedClient
              .newBuilder()
              .socketFactory(new AFUNIXSocketFactory.FactoryArg(spec.unixDomainSocket()))
              // Enable HTTP/2 if available (uses H2 upgrade),
              // otherwise fallback to HTTP/1.1
              .protocols(Collections.singletonList(Protocol.HTTP_2))
              .callTimeout(spec.maxRequestDuration())
              .build();

      return new HttpRequestReplyClient(
          // Only the path matters!
          HttpUrl.get(URI.create(spec.endpoint().getPath())), specificClient);
    } else {
      // specific client reuses the same the connection pool and thread pool
      // as the sharedClient.
      OkHttpClient specificClient =
          sharedClient.newBuilder().callTimeout(spec.maxRequestDuration()).build();
      return new HttpRequestReplyClient(HttpUrl.get(spec.endpoint()), specificClient);
    }
  }
}
