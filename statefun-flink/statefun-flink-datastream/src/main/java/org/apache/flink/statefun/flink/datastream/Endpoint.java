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

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionEndpointSpec.Target;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionEndpointSpec.UrlPathTemplate;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;

/**
 * Specifies an endpoint for connecting to a remote function. This spec corresponds to the {@code
 * endpoints} section of the {@code module.yaml} configuration file.The specification correlates the
 * logical typename of a function to a physical endpoints.
 *
 * <p>The {@code functions} parameter of the Endpoint is the {@link TypeName} of the logical
 * Stateful Functions. This parameter can points towards a fully qualified typename - {@literal
 * '<namespace>/<name>'} or contain a wildcard in the name position - {@literal '<namespace>/*'}.
 * Wildcard endpoints will match all functions under the given namespace.
 *
 * <p>The {@code urlPathTemplate} parameter is the physical URL under which the functions are
 * available. Path templates may contain template parameters that are filled in based on the
 * functions specific type. Template parameterization works well with load balancers and service
 * gateways.
 *
 * <p>For example, a message sent to the typename {@literal 'com.example/greeter'} will be routed to
 * {@literal 'http://bar.foo.com/greeter'}.
 *
 * <p>{@code Endpoint.withSpec("example/*", "https://endpoints/{function.name}"}</pre>
 */
public class Endpoint {

  private static final String INVALID_SYNTAX_MESSAGE =
      "Invalid syntax for functions. Only <namespace>/<name> or <namespace>/* are supported.";

  private final DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec =
      new DefaultHttpRequestReplyClientSpec.Timeouts();

  /** Creates a new Endpoint based on the given specification. */
  public static Endpoint withSpec(String functions, String urlPathTemplate) {
    Objects.requireNonNull(functions, "functions cannot be null");
    Objects.requireNonNull(urlPathTemplate, "urlPathTemplate cannot be null");
    return new Endpoint(asTarget(functions), new UrlPathTemplate(urlPathTemplate));
  }

  private static Target asTarget(String functions) {
    TypeName targetTypeName = TypeName.parseFrom(functions);
    if (targetTypeName.namespace().contains("*")) {
      throw new IllegalArgumentException(INVALID_SYNTAX_MESSAGE);
    }
    if (targetTypeName.name().equals("*")) {
      return FunctionEndpointSpec.Target.namespace(targetTypeName.namespace());
    }
    if (targetTypeName.name().contains("*")) {
      throw new IllegalArgumentException(INVALID_SYNTAX_MESSAGE);
    }
    FunctionType functionType = new FunctionType(targetTypeName.namespace(), targetTypeName.name());
    return FunctionEndpointSpec.Target.functionType(functionType);
  }

  private final HttpFunctionEndpointSpec.Builder builder;

  private Endpoint(Target functions, UrlPathTemplate urlPathTemplate) {
    this.builder = HttpFunctionEndpointSpec.builder(functions, urlPathTemplate);
  }

  /**
   * Set a maximum request duration. This duration spans the complete call, including connecting to
   * the function endpoint, writing the request, function processing, and reading the response.
   *
   * @param duration the duration after which the request is considered failed.
   * @return this builder.
   */
  public Endpoint withMaxRequestDuration(Duration duration) {
    transportClientTimeoutsSpec.setCallTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for connecting to function endpoints.
   *
   * @param duration the duration after which a connect attempt is considered failed.
   * @return this builder.
   */
  public Endpoint withConnectTimeout(Duration duration) {
    transportClientTimeoutsSpec.setConnectTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for individual read IO operations during a function invocation request.
   *
   * @param duration the duration after which a read IO operation is considered failed.
   * @return this builder.
   */
  public Endpoint withReadTimeout(Duration duration) {
    transportClientTimeoutsSpec.setReadTimeout(duration);
    return this;
  }

  /**
   * Set a timeout for individual write IO operations during a function invocation request.
   *
   * @param duration the duration after which a write IO operation is considered failed.
   * @return this builder.
   */
  public Endpoint withWriteTimeout(Duration duration) {
    transportClientTimeoutsSpec.setWriteTimeout(duration);
    return this;
  }

  /**
   * Sets the max messages to batch together for a specific address.
   *
   * @param maxNumBatchRequests the maximum number of requests to batch for an address.
   * @return this builder.
   */
  public Endpoint withMaxNumBatchRequests(int maxNumBatchRequests) {
    builder.withMaxNumBatchRequests(maxNumBatchRequests);
    return this;
  }

  @Internal
  HttpFunctionEndpointSpec spec() {
    builder.withTransportClientFactoryType(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE);
    builder.withTransportClientProperties(
        transportClientPropertiesAsObjectNode(transportClientTimeoutsSpec));
    return builder.build();
  }

  private static ObjectNode transportClientPropertiesAsObjectNode(
      DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec) {
    final DefaultHttpRequestReplyClientSpec transportClientSpecPojo =
        new DefaultHttpRequestReplyClientSpec();
    transportClientSpecPojo.setTimeouts(transportClientTimeoutsSpec);

    return new ObjectMapper().valueToTree(transportClientSpecPojo);
  }
}
