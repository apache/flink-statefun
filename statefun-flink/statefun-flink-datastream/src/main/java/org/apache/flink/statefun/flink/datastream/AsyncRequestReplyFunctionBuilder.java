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

package org.apache.flink.statefun.flink.datastream;

import java.net.URI;
import java.time.Duration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.*;
import org.apache.flink.statefun.flink.core.nettyclient.NettyRequestReplySpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.TimeUtils;

/** A builder for async RequestReply remote function type. */
public class AsyncRequestReplyFunctionBuilder extends StatefulFunctionBuilder {

  private final ObjectNode transportProperties;

  private final HttpFunctionEndpointSpec.Builder builder;

  AsyncRequestReplyFunctionBuilder(FunctionType functionType, URI endpoint) {
    this.transportProperties = CLIENT_SPEC_OBJ_MAPPER.createObjectNode();
    this.builder =
        HttpFunctionEndpointSpec.builder(
            TargetFunctions.functionType(functionType),
            new UrlPathTemplate(endpoint.toASCIIString()));
  }

  /**
   * Set a maximum request duration. This duration spans the complete call, including connecting to
   * the function endpoint, writing the request, function processing, and reading the response.
   *
   * @param duration the duration after which the request is considered failed.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withMaxRequestDuration(Duration duration) {
    transportProperties.put(
        NettyRequestReplySpec.CALL_TIMEOUT_PROPERTY, TimeUtils.formatWithHighestUnit(duration));
    return this;
  }

  /**
   * Set a timeout for connecting to function endpoints.
   *
   * @param duration the duration after which a connect attempt is considered failed.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withConnectTimeout(Duration duration) {
    transportProperties.put(
        NettyRequestReplySpec.CONNECT_TIMEOUT_PROPERTY, TimeUtils.formatWithHighestUnit(duration));
    return this;
  }

  /**
   * The amount of time a connection will live in the connection pool. Set to zero to disable, the
   * connection will be evicted from the pool after that time.
   *
   * @param duration the duration after which a connection will be evicted from the pool.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withPooledConnectionTTL(Duration duration) {
    transportProperties.put(
        NettyRequestReplySpec.POOLED_CONNECTION_TTL_PROPERTY,
        TimeUtils.formatWithHighestUnit(duration));
    return this;
  }

  /**
   * The maximum connection pool size.
   *
   * @param size the max size of the connection pool.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withConnectionPoolMaxSize(int size) {
    transportProperties.put(NettyRequestReplySpec.CONNECTION_POOL_MAX_SIZE_PROPERTY, size);
    return this;
  }

  /**
   * The maximum size for a request or response payload.
   *
   * @param maxSizeInBytes the max size of the request or response payload.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withMaxRequestOrResponseSizeInBytes(int maxSizeInBytes) {
    transportProperties.put(
        NettyRequestReplySpec.MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES_PROPERTY, maxSizeInBytes);
    return this;
  }

  /**
   * Sets the max messages to batch together for a specific address.
   *
   * @param maxNumBatchRequests the maximum number of requests to batch for an address.
   * @return this builder.
   */
  public AsyncRequestReplyFunctionBuilder withMaxNumBatchRequests(int maxNumBatchRequests) {
    builder.withMaxNumBatchRequests(maxNumBatchRequests);
    return this;
  }

  /**
   * Create the endpoint spec for the function.
   *
   * @return The endpoint spec.
   */
  @Internal
  @Override
  HttpFunctionEndpointSpec spec() {
    final TransportClientSpec transportClientSpec =
        new TransportClientSpec(
            TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE, transportProperties);
    builder.withTransport(transportClientSpec);
    return builder.build();
  }
}
