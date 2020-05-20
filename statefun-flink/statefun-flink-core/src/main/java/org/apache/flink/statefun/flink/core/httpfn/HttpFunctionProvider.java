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

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

import java.util.Map;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

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
    OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();
    clientBuilder.callTimeout(spec.maxRequestDuration());

    final HttpUrl url;
    if (spec.isUnixDomainSocket()) {
      UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(spec.endpoint());

      url =
          new HttpUrl.Builder()
              .scheme("http")
              .host("unused")
              .addPathSegment(endpoint.pathSegment)
              .build();

      configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
    } else {
      url = HttpUrl.get(spec.endpoint());
    }
    return new HttpRequestReplyClient(url, clientBuilder.build());
  }
}
