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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.nettyclient.NettyRequestReplySpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.Test;

public class AsyncRequestReplyFunctionBuilderTest {

  /** Test that an asynchronous client spec can be created specifying all values */
  @Test
  public void asyncClientSpecCanBeCreatedWithAllValues()
      throws URISyntaxException, JsonProcessingException {

    final FunctionType functionType = new FunctionType("foobar", "barfoo");
    final URI uri = new URI("foobar");

    final int maxNumBatchRequests = 100;
    final int maxRetries = 5;
    final Duration connectTimeout = Duration.ofSeconds(1);
    final Duration callTimeout = Duration.ofSeconds(2);
    final Duration pooledConnectionTTL = Duration.ofSeconds(3);
    final int connectionPoolMaxSize = 10;
    final int maxRequestOrResponseSizeInBytes = 10000;

    final AsyncRequestReplyFunctionBuilder builder =
        StatefulFunctionBuilder.asyncRequestReplyFunctionBuilder(functionType, uri)
            .withMaxNumBatchRequests(maxNumBatchRequests)
            .withMaxRetries(maxRetries)
            .withMaxRequestDuration(callTimeout)
            .withConnectTimeout(connectTimeout)
            .withPooledConnectionTTL(pooledConnectionTTL)
            .withConnectionPoolMaxSize(connectionPoolMaxSize)
            .withMaxRequestOrResponseSizeInBytes(maxRequestOrResponseSizeInBytes);

    HttpFunctionEndpointSpec spec = builder.spec();
    assertThat(spec, notNullValue());
    assertEquals(maxNumBatchRequests, spec.maxNumBatchRequests());
    assertEquals(maxRetries, spec.maxRetries());
    assertEquals(functionType, spec.targetFunctions().asSpecificFunctionType());
    assertEquals(
        spec.transportClientFactoryType(), TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
    assertEquals(uri, spec.urlPathTemplate().apply(functionType));

    ObjectNode transportClientProperties = spec.transportClientProperties();
    NettyRequestReplySpec nettySpec =
        StatefulFunctionBuilder.CLIENT_SPEC_OBJ_MAPPER.treeToValue(
            transportClientProperties, NettyRequestReplySpec.class);
    assertThat(nettySpec, notNullValue());
    assertEquals(callTimeout, nettySpec.callTimeout);
    assertEquals(connectTimeout, nettySpec.connectTimeout);
    assertEquals(pooledConnectionTTL, nettySpec.pooledConnectionTTL);
    assertEquals(connectionPoolMaxSize, nettySpec.connectionPoolMaxSize);
    assertEquals(maxRequestOrResponseSizeInBytes, nettySpec.maxRequestOrResponseSizeInBytes);
  }

  /**
   * Test that an asynchronous client spec can be created specifying some values, using defaults for
   * others.
   */
  @Test
  public void asyncClientSpecCanBeCreatedWithSomeValues()
      throws URISyntaxException, JsonProcessingException {

    final FunctionType functionType = new FunctionType("foobar", "barfoo");
    final URI uri = new URI("foobar");

    final int maxNumBatchRequests = 100;
    final int maxRetries = 5;
    final Duration callTimeout = Duration.ofSeconds(2);
    final Duration pooledConnectionTTL = Duration.ofSeconds(3);
    final int maxRequestOrResponseSizeInBytes = 10000;

    final AsyncRequestReplyFunctionBuilder builder =
        StatefulFunctionBuilder.asyncRequestReplyFunctionBuilder(functionType, uri)
            .withMaxNumBatchRequests(maxNumBatchRequests)
            .withMaxRetries(maxRetries)
            .withMaxRequestDuration(callTimeout)
            .withPooledConnectionTTL(pooledConnectionTTL)
            .withMaxRequestOrResponseSizeInBytes(maxRequestOrResponseSizeInBytes);

    HttpFunctionEndpointSpec spec = builder.spec();
    assertThat(spec, notNullValue());
    assertEquals(maxNumBatchRequests, spec.maxNumBatchRequests());
    assertEquals(maxRetries, spec.maxRetries());
    assertEquals(functionType, spec.targetFunctions().asSpecificFunctionType());
    assertEquals(
        spec.transportClientFactoryType(), TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE);
    assertEquals(uri, spec.urlPathTemplate().apply(functionType));

    ObjectNode transportClientProperties = spec.transportClientProperties();
    NettyRequestReplySpec nettySpec =
        StatefulFunctionBuilder.CLIENT_SPEC_OBJ_MAPPER.treeToValue(
            transportClientProperties, NettyRequestReplySpec.class);
    assertThat(nettySpec, notNullValue());
    assertEquals(callTimeout, nettySpec.callTimeout);
    assertEquals(NettyRequestReplySpec.DEFAULT_CONNECT_TIMEOUT, nettySpec.connectTimeout);
    assertEquals(pooledConnectionTTL, nettySpec.pooledConnectionTTL);
    assertEquals(
        NettyRequestReplySpec.DEFAULT_CONNECTION_POOL_MAX_SIZE, nettySpec.connectionPoolMaxSize);
    assertEquals(maxRequestOrResponseSizeInBytes, nettySpec.maxRequestOrResponseSizeInBytes);
  }
}
