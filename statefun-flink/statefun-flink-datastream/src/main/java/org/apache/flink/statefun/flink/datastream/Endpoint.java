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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientSpec;
import org.apache.flink.statefun.flink.core.httpfn.UrlPathTemplate;
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
 * <p>{@code Endpoint.http("example/*", "https://endpoints/{function.name}"}</pre>
 */
public abstract class Endpoint {

  private final TargetFunctions targetFunctions;

  private Endpoint(TargetFunctions targetFunctions) {
    this.targetFunctions = targetFunctions;
  }

  /** Creates a new http endpoint based on the given specification. */
  public static HttpEndpoint http(String functions, String urlPathTemplate) {
    Objects.requireNonNull(functions, "functions cannot be null");
    Objects.requireNonNull(urlPathTemplate, "urlPathTemplate cannot be null");
    TargetFunctions targetFunctions = TargetFunctions.fromPatternString(functions);

    return new HttpEndpoint(targetFunctions, new UrlPathTemplate(urlPathTemplate));
  }

  @Internal
  abstract SerializableStatefulFunctionProvider getFunctionProvider();

  @Internal
  final TargetFunctions targetFunctions() {
    return targetFunctions;
  }

  public static class HttpEndpoint extends Endpoint {

    private final DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec =
        new DefaultHttpRequestReplyClientSpec.Timeouts();

    private final HttpFunctionEndpointSpec.Builder builder;

    private HttpEndpoint(TargetFunctions functions, UrlPathTemplate urlPathTemplate) {
      super(functions);
      this.builder = HttpFunctionEndpointSpec.builder(functions, urlPathTemplate);
    }

    /**
     * Set a maximum request duration. This duration spans the complete call, including connecting
     * to the function endpoint, writing the request, function processing, and reading the response.
     *
     * @param duration the duration after which the request is considered failed.
     * @return this builder.
     */
    public HttpEndpoint withMaxRequestDuration(Duration duration) {
      transportClientTimeoutsSpec.setCallTimeout(duration);
      return this;
    }

    /**
     * Set a timeout for connecting to function endpoints.
     *
     * @param duration the duration after which a connect attempt is considered failed.
     * @return this builder.
     */
    public HttpEndpoint withConnectTimeout(Duration duration) {
      transportClientTimeoutsSpec.setConnectTimeout(duration);
      return this;
    }

    /**
     * Set a timeout for individual read IO operations during a function invocation request.
     *
     * @param duration the duration after which a read IO operation is considered failed.
     * @return this builder.
     */
    public HttpEndpoint withReadTimeout(Duration duration) {
      transportClientTimeoutsSpec.setReadTimeout(duration);
      return this;
    }

    /**
     * Set a timeout for individual write IO operations during a function invocation request.
     *
     * @param duration the duration after which a write IO operation is considered failed.
     * @return this builder.
     */
    public HttpEndpoint withWriteTimeout(Duration duration) {
      transportClientTimeoutsSpec.setWriteTimeout(duration);
      return this;
    }

    /**
     * Sets the max messages to batch together for a specific address.
     *
     * @param maxNumBatchRequests the maximum number of requests to batch for an address.
     * @return this builder.
     */
    public HttpEndpoint withMaxNumBatchRequests(int maxNumBatchRequests) {
      builder.withMaxNumBatchRequests(maxNumBatchRequests);
      return this;
    }

    @Override
    SerializableStatefulFunctionProvider getFunctionProvider() {
      final TransportClientSpec transportClientSpec =
          new TransportClientSpec(
              TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE,
              transportClientPropertiesAsObjectNode(transportClientTimeoutsSpec));
      builder.withTransport(transportClientSpec);
      return new SerializableHttpFunctionProvider(builder.build());
    }

    private static ObjectNode transportClientPropertiesAsObjectNode(
        DefaultHttpRequestReplyClientSpec.Timeouts transportClientTimeoutsSpec) {
      final DefaultHttpRequestReplyClientSpec transportClientSpecPojo =
          new DefaultHttpRequestReplyClientSpec();
      transportClientSpecPojo.setTimeouts(transportClientTimeoutsSpec);

      return StateFunObjectMapper.create().valueToTree(transportClientSpecPojo);
    }
  }
}
