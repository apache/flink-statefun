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

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

@NotThreadSafe
public final class HttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {

  private final Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs;
  private final Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs;

  /** lazily initialized by {code buildHttpClient} */
  @Nullable private OkHttpClient sharedClient;

  private volatile boolean shutdown;

  public HttpFunctionProvider(
      Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs,
      Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs) {
    this.specificTypeEndpointSpecs = Objects.requireNonNull(specificTypeEndpointSpecs);
    this.perNamespaceEndpointSpecs = Objects.requireNonNull(perNamespaceEndpointSpecs);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    final HttpFunctionEndpointSpec endpointsSpec = getEndpointsSpecOrThrow(functionType);
    return new RequestReplyFunction(
        endpointsSpec.maxNumBatchRequests(), buildHttpClient(endpointsSpec, functionType));
  }

  private HttpFunctionEndpointSpec getEndpointsSpecOrThrow(FunctionType functionType) {
    HttpFunctionEndpointSpec endpointSpec = specificTypeEndpointSpecs.get(functionType);
    if (endpointSpec != null) {
      return endpointSpec;
    }
    endpointSpec = perNamespaceEndpointSpecs.get(functionType.namespace());
    if (endpointSpec != null) {
      return endpointSpec;
    }

    throw new IllegalStateException("Unknown type: " + functionType);
  }

  private RequestReplyClient buildHttpClient(
      HttpFunctionEndpointSpec spec, FunctionType functionType) {
    if (sharedClient == null) {
      sharedClient = OkHttpUtils.newClient();
    }
    OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();
    clientBuilder.callTimeout(spec.maxRequestDuration());
    clientBuilder.connectTimeout(spec.connectTimeout());
    clientBuilder.readTimeout(spec.readTimeout());
    clientBuilder.writeTimeout(spec.writeTimeout());

    URI endpointUrl = spec.urlPathTemplate().apply(functionType);

    final HttpUrl url;
    if (UnixDomainHttpEndpoint.validate(endpointUrl)) {
      UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(endpointUrl);

      url =
          new HttpUrl.Builder()
              .scheme("http")
              .host("unused")
              .addPathSegment(endpoint.pathSegment)
              .build();

      configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
    } else {
      url = HttpUrl.get(endpointUrl);
    }
    return new HttpRequestReplyClient(url, clientBuilder.build(), () -> shutdown);
  }

  @Override
  public void shutdown() {
    shutdown = true;
    OkHttpUtils.closeSilently(sharedClient);
  }
}
