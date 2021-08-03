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
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

@NotThreadSafe
public final class HttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {

  private final HttpFunctionEndpointSpec endpointSpec;
  private final RequestReplyClientFactory requestReplyClientFactory;

  public HttpFunctionProvider(
      HttpFunctionEndpointSpec endpointSpec, RequestReplyClientFactory requestReplyClientFactory) {
    this.endpointSpec = Objects.requireNonNull(endpointSpec);
    this.requestReplyClientFactory = Objects.requireNonNull(requestReplyClientFactory);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    final URI endpointUrl = endpointSpec.urlPathTemplate().apply(functionType);

    return new RequestReplyFunction(
        endpointSpec.maxNumBatchRequests(),
        requestReplyClientFactory.createTransportClient(
            endpointSpec.transportClientProperties(), endpointUrl));
  }

  @Override
  public void shutdown() {
    requestReplyClientFactory.cleanup();
  }
}
