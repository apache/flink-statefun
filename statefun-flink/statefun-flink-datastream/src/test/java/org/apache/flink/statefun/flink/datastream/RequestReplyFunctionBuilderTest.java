package org.apache.flink.statefun.flink.datastream;

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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.Test;

public class RequestReplyFunctionBuilderTest {

  /** Test that a synchronous client spec can be created specifying all values. */
  @Test
  public void clientSpecCanBeCreatedWithAllValues()
      throws URISyntaxException, JsonProcessingException {
    final FunctionType functionType = new FunctionType("foobar", "barfoo");
    final URI uri = new URI("foobar");

    final int maxNumBatchRequests = 100;
    final int maxRetries = 5;
    final Duration connectTimeout = Duration.ofSeconds(16);
    final Duration callTimeout = Duration.ofSeconds(21);
    final Duration readTimeout = Duration.ofSeconds(11);
    final Duration writeTimeout = Duration.ofSeconds(12);

    final RequestReplyFunctionBuilder builder =
        StatefulFunctionBuilder.requestReplyFunctionBuilder(functionType, uri)
            .withMaxNumBatchRequests(maxNumBatchRequests)
            .withMaxRetries(maxRetries)
            .withMaxRequestDuration(callTimeout)
            .withConnectTimeout(connectTimeout)
            .withReadTimeout(readTimeout)
            .withWriteTimeout(writeTimeout);

    HttpFunctionEndpointSpec spec = builder.spec();
    assertThat(spec, notNullValue());
    assertEquals(maxNumBatchRequests, spec.maxNumBatchRequests());
    assertEquals(maxRetries, spec.maxRetries());
    assertEquals(functionType, spec.targetFunctions().asSpecificFunctionType());
    assertEquals(
        spec.transportClientFactoryType(), TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE);
    assertEquals(uri, spec.urlPathTemplate().apply(functionType));

    ObjectNode transportClientProperties = spec.transportClientProperties();
    DefaultHttpRequestReplyClientSpec clientSpec =
        StatefulFunctionBuilder.CLIENT_SPEC_OBJ_MAPPER.treeToValue(
            transportClientProperties, DefaultHttpRequestReplyClientSpec.class);
    assertThat(clientSpec, notNullValue());
    DefaultHttpRequestReplyClientSpec.Timeouts timeouts = clientSpec.getTimeouts();
    assertEquals(callTimeout, timeouts.getCallTimeout());
    assertEquals(connectTimeout, timeouts.getConnectTimeout());
    assertEquals(readTimeout, timeouts.getReadTimeout());
    assertEquals(writeTimeout, timeouts.getWriteTimeout());
  }

  /**
   * Test that a synchronous client spec can be created specifying some values, using defaults for
   * others.
   */
  @Test
  public void clientSpecCanBeCreatedWithSomeValues()
      throws URISyntaxException, JsonProcessingException {
    final FunctionType functionType = new FunctionType("foobar", "barfoo");
    final URI uri = new URI("foobar");

    final int maxNumBatchRequests = 100;
    final int maxRetries = 5;
    final Duration connectTimeout = Duration.ofSeconds(16);
    final Duration callTimeout = Duration.ofSeconds(21);

    final RequestReplyFunctionBuilder builder =
        StatefulFunctionBuilder.requestReplyFunctionBuilder(functionType, uri)
            .withMaxNumBatchRequests(maxNumBatchRequests)
            .withMaxRetries(maxRetries)
            .withMaxRequestDuration(callTimeout)
            .withConnectTimeout(connectTimeout);

    HttpFunctionEndpointSpec spec = builder.spec();
    assertThat(spec, notNullValue());
    assertEquals(maxNumBatchRequests, spec.maxNumBatchRequests());
    assertEquals(maxRetries, spec.maxRetries());
    assertEquals(functionType, spec.targetFunctions().asSpecificFunctionType());
    assertEquals(
        spec.transportClientFactoryType(), TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE);
    assertEquals(uri, spec.urlPathTemplate().apply(functionType));

    ObjectNode transportClientProperties = spec.transportClientProperties();
    DefaultHttpRequestReplyClientSpec clientSpec =
        StatefulFunctionBuilder.CLIENT_SPEC_OBJ_MAPPER.treeToValue(
            transportClientProperties, DefaultHttpRequestReplyClientSpec.class);
    assertThat(clientSpec, notNullValue());
    DefaultHttpRequestReplyClientSpec.Timeouts timeouts = clientSpec.getTimeouts();
    assertEquals(callTimeout, timeouts.getCallTimeout());
    assertEquals(connectTimeout, timeouts.getConnectTimeout());
    assertEquals(
        DefaultHttpRequestReplyClientSpec.Timeouts.DEFAULT_HTTP_READ_TIMEOUT,
        timeouts.getReadTimeout());
    assertEquals(
        DefaultHttpRequestReplyClientSpec.Timeouts.DEFAULT_HTTP_WRITE_TIMEOUT,
        timeouts.getWriteTimeout());
  }

  /** Test that a synchronous client spec can be created via the deprecated method. */
  @Test
  public void clientSpecCanBeCreatedViaDeprecatedMethod() throws URISyntaxException {
    final RequestReplyFunctionBuilder requestReplyFunctionBuilder =
        RequestReplyFunctionBuilder.requestReplyFunctionBuilder(
            new FunctionType("foobar", "barfoo"), new URI("foobar"));

    assertThat(requestReplyFunctionBuilder.spec(), notNullValue());
  }
}
