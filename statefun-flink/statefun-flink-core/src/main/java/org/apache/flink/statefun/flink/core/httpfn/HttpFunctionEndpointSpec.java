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

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.httpfn.jsonutils.TargetFunctionsJsonDeserializer;
import org.apache.flink.statefun.flink.core.httpfn.jsonutils.UrlPathTemplateJsonDeserializer;
import org.apache.flink.statefun.sdk.TypeName;

@JsonDeserialize(builder = HttpFunctionEndpointSpec.Builder.class)
public final class HttpFunctionEndpointSpec implements Serializable {

  private static final long serialVersionUID = 1;

  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;

  private static final Integer DEFAULT_MAX_RETRIES = -1;

  private static final TransportClientSpec DEFAULT_TRANSPORT_CLIENT_SPEC =
      new TransportClientSpec(
          TransportClientConstants.ASYNC_CLIENT_FACTORY_TYPE,
          new ObjectMapper().createObjectNode());

  // ============================================================
  //  Request-Reply invocation protocol configurations
  // ============================================================

  private final TargetFunctions targetFunctions;
  private final UrlPathTemplate urlPathTemplate;
  private final int maxNumBatchRequests;
  private final int maxRetries;

  // ============================================================
  //  HTTP transport related properties
  // ============================================================

  private final TypeName transportClientFactoryType;
  private final ObjectNode transportClientProps;

  public static Builder builder(TargetFunctions targetFunctions, UrlPathTemplate urlPathTemplate) {
    return new Builder(targetFunctions, urlPathTemplate);
  }

  private HttpFunctionEndpointSpec(
      TargetFunctions targetFunctions,
      UrlPathTemplate urlPathTemplate,
      int maxNumBatchRequests,
      int maxRetries,
      TypeName transportClientFactoryType,
      ObjectNode transportClientProps) {
    this.targetFunctions = targetFunctions;
    this.urlPathTemplate = urlPathTemplate;
    this.maxNumBatchRequests = maxNumBatchRequests;
    this.maxRetries = maxRetries;
    this.transportClientFactoryType = transportClientFactoryType;
    this.transportClientProps = transportClientProps;
  }

  public TargetFunctions targetFunctions() {
    return targetFunctions;
  }

  public UrlPathTemplate urlPathTemplate() {
    return urlPathTemplate;
  }

  public int maxNumBatchRequests() {
    return maxNumBatchRequests;
  }

  public int maxRetries() {
    return maxRetries;
  }

  public TypeName transportClientFactoryType() {
    return transportClientFactoryType;
  }

  public ObjectNode transportClientProperties() {
    return transportClientProps;
  }

  @JsonPOJOBuilder
  public static final class Builder {

    private final TargetFunctions targetFunctions;
    private final UrlPathTemplate urlPathTemplate;

    private int maxNumBatchRequests = DEFAULT_MAX_NUM_BATCH_REQUESTS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private TransportClientSpec transportClientSpec = DEFAULT_TRANSPORT_CLIENT_SPEC;

    @JsonCreator
    private Builder(
        @JsonProperty("functions") @JsonDeserialize(using = TargetFunctionsJsonDeserializer.class)
            TargetFunctions targetFunctions,
        @JsonProperty("urlPathTemplate")
            @JsonDeserialize(using = UrlPathTemplateJsonDeserializer.class)
            UrlPathTemplate urlPathTemplate) {
      this.targetFunctions = Objects.requireNonNull(targetFunctions);
      this.urlPathTemplate = Objects.requireNonNull(urlPathTemplate);
    }

    @JsonProperty("maxNumBatchRequests")
    public Builder withMaxNumBatchRequests(int maxNumBatchRequests) {
      this.maxNumBatchRequests = maxNumBatchRequests;
      return this;
    }

    @JsonProperty("maxRetries")
    public Builder withMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * This is marked with @JsonProperty specifically to tell Jackson to use this method when
     * deserializing from Json.
     */
    @JsonProperty("transport")
    public Builder withTransport(ObjectNode transportNode) {
      withTransport(TransportClientSpec.fromJsonNode(transportNode));
      return this;
    }

    public Builder withTransport(TransportClientSpec transportNode) {
      this.transportClientSpec = Objects.requireNonNull(transportNode);
      return this;
    }

    public HttpFunctionEndpointSpec build() {
      return new HttpFunctionEndpointSpec(
          targetFunctions,
          urlPathTemplate,
          maxNumBatchRequests,
          maxRetries,
          transportClientSpec.factoryKind(),
          transportClientSpec.specNode());
    }
  }
}
