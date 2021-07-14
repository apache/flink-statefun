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
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.ContextSafeRequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

@NotThreadSafe
public final class HttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {

  private final Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs;
  private final Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs;

  public HttpFunctionProvider(
      Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs,
      Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs) {
    this.specificTypeEndpointSpecs = Objects.requireNonNull(specificTypeEndpointSpecs);
    this.perNamespaceEndpointSpecs = Objects.requireNonNull(perNamespaceEndpointSpecs);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    final HttpFunctionEndpointSpec endpointsSpec = getEndpointsSpecOrThrow(functionType);
    final URI endpointUrl = endpointsSpec.urlPathTemplate().apply(functionType);

    return new RequestReplyFunction(
        endpointsSpec.maxNumBatchRequests(),
        buildTransportClientFromSpec(endpointUrl, endpointsSpec));
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

  private static RequestReplyClient buildTransportClientFromSpec(
      URI endpointUrl, HttpFunctionEndpointSpec endpointsSpec) {
    final RequestReplyClientFactory factory = endpointsSpec.transportClientFactory();
    final ObjectNode properties = endpointsSpec.transportClientProperties();

    if (Thread.currentThread().getContextClassLoader() == factory.getClass().getClassLoader()) {
      // in this case, we're using one of our own shipped transport client factory
      return factory.createTransportClient(properties, endpointUrl);
    } else {
      try (SetContextClassLoader ignored = new SetContextClassLoader(factory)) {
        return new ContextSafeRequestReplyClient(
            factory.createTransportClient(properties, endpointUrl));
      }
    }
  }

  @Override
  public void shutdown() {
    specificTypeEndpointSpecs
        .values()
        .forEach(spec -> shutdownTransportClientFactory(spec.transportClientFactory()));
    perNamespaceEndpointSpecs
        .values()
        .forEach(spec -> shutdownTransportClientFactory(spec.transportClientFactory()));
  }

  private static void shutdownTransportClientFactory(RequestReplyClientFactory factory) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(factory)) {
      factory.cleanup();
    }
  }
}
