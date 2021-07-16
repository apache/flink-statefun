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
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.TypeName;

@NotThreadSafe
public final class HttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {

  private final Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs;
  private final Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs;

  private final ExtensionResolver extensionResolver;

  public HttpFunctionProvider(
      Map<FunctionType, HttpFunctionEndpointSpec> specificTypeEndpointSpecs,
      Map<String, HttpFunctionEndpointSpec> perNamespaceEndpointSpecs,
      ExtensionResolver extensionResolver) {
    this.specificTypeEndpointSpecs = Objects.requireNonNull(specificTypeEndpointSpecs);
    this.perNamespaceEndpointSpecs = Objects.requireNonNull(perNamespaceEndpointSpecs);
    this.extensionResolver = Objects.requireNonNull(extensionResolver);
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

  private RequestReplyClient buildTransportClientFromSpec(
      URI endpointUrl, HttpFunctionEndpointSpec endpointsSpec) {
    final TypeName factoryType = endpointsSpec.transportClientFactoryType();
    final ObjectNode properties = endpointsSpec.transportClientProperties();

    final RequestReplyClientFactory factory =
        extensionResolver.resolveExtension(factoryType, RequestReplyClientFactory.class);
    return factory.createTransportClient(properties, endpointUrl);
  }

  @Override
  public void shutdown() {
    // TODO all RequestReplyClientFactory's need to be shutdown.
    // TODO This should probably happen in StatefulFunctionsUniverse.
  }
}
