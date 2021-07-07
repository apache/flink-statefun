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

import static org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionEndpointSpec;
import org.apache.flink.statefun.sdk.TypeName;

public final class HttpFunctionEndpointSpec implements FunctionEndpointSpec, Serializable {

  private static final long serialVersionUID = 1;

  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;

  // ============================================================
  //  Request-Reply invocation protocol configurations
  // ============================================================

  private final Target target;
  private final UrlPathTemplate urlPathTemplate;
  private final int maxNumBatchRequests;

  // ============================================================
  //  HTTP transport related properties
  // ============================================================

  private final TypeName transportClientFactoryType;
  private final ObjectNode transportClientProps;

  public static Builder builder(Target target, UrlPathTemplate urlPathTemplate) {
    return new Builder(target, urlPathTemplate);
  }

  private HttpFunctionEndpointSpec(
      Target target,
      UrlPathTemplate urlPathTemplate,
      int maxNumBatchRequests,
      TypeName transportClientFactoryType,
      ObjectNode transportClientProps) {
    this.target = target;
    this.urlPathTemplate = urlPathTemplate;
    this.maxNumBatchRequests = maxNumBatchRequests;
    this.transportClientFactoryType = transportClientFactoryType;
    this.transportClientProps = transportClientProps;
  }

  @Override
  public Target target() {
    return target;
  }

  @Override
  public Kind kind() {
    return Kind.HTTP;
  }

  @Override
  public UrlPathTemplate urlPathTemplate() {
    return urlPathTemplate;
  }

  public int maxNumBatchRequests() {
    return maxNumBatchRequests;
  }

  public TypeName transportClientFactoryType() {
    return transportClientFactoryType;
  }

  public ObjectNode transportClientProperties() {
    return transportClientProps;
  }

  public static final class Builder {

    private final Target target;
    private final UrlPathTemplate urlPathTemplate;
    private int maxNumBatchRequests = DEFAULT_MAX_NUM_BATCH_REQUESTS;

    private TypeName transportClientFactoryType = OKHTTP_CLIENT_FACTORY_TYPE;
    private ObjectNode transportClientProperties = new ObjectMapper().createObjectNode();

    private Builder(Target target, UrlPathTemplate urlPathTemplate) {
      this.target = Objects.requireNonNull(target);
      this.urlPathTemplate = Objects.requireNonNull(urlPathTemplate);
    }

    public Builder withMaxNumBatchRequests(int maxNumBatchRequests) {
      this.maxNumBatchRequests = maxNumBatchRequests;
      return this;
    }

    public Builder withTransportClientFactoryType(TypeName transportClientFactoryType) {
      this.transportClientFactoryType = Objects.requireNonNull(transportClientFactoryType);
      return this;
    }

    public Builder withTransportClientProperties(ObjectNode transportClientProperties) {
      this.transportClientProperties = Objects.requireNonNull(transportClientProperties);
      return this;
    }

    public HttpFunctionEndpointSpec build() {

      return new HttpFunctionEndpointSpec(
          target,
          urlPathTemplate,
          maxNumBatchRequests,
          transportClientFactoryType,
          transportClientProperties);
    }
  }
}
