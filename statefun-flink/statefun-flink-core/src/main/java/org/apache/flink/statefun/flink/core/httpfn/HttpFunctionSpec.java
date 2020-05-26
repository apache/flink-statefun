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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec;
import org.apache.flink.statefun.sdk.FunctionType;

public final class HttpFunctionSpec implements FunctionSpec {

  private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);
  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;

  private final FunctionType functionType;
  private final URI endpoint;
  private final List<String> states;
  private final Duration maxRequestDuration;
  private final int maxNumBatchRequests;

  private HttpFunctionSpec(
      FunctionType functionType,
      URI endpoint,
      List<String> states,
      Duration maxRequestDuration,
      int maxNumBatchRequests) {
    this.functionType = Objects.requireNonNull(functionType);
    this.endpoint = Objects.requireNonNull(endpoint);
    this.states = Objects.requireNonNull(states);
    this.maxRequestDuration = Objects.requireNonNull(maxRequestDuration);
    this.maxNumBatchRequests = maxNumBatchRequests;
  }

  public static Builder builder(FunctionType functionType, URI endpoint) {
    return new Builder(functionType, endpoint);
  }

  @Override
  public FunctionType functionType() {
    return functionType;
  }

  @Override
  public Kind kind() {
    return Kind.HTTP;
  }

  public URI endpoint() {
    return endpoint;
  }

  public boolean isUnixDomainSocket() {
    String scheme = endpoint.getScheme();
    return "http+unix".equalsIgnoreCase(scheme) || "https+unix".equalsIgnoreCase(scheme);
  }

  public List<String> states() {
    return states;
  }

  public Duration maxRequestDuration() {
    return maxRequestDuration;
  }

  public int maxNumBatchRequests() {
    return maxNumBatchRequests;
  }

  public static final class Builder {

    private final FunctionType functionType;
    private final URI endpoint;

    private final List<String> states = new ArrayList<>();
    private Duration maxRequestDuration = DEFAULT_HTTP_TIMEOUT;
    private int maxNumBatchRequests = DEFAULT_MAX_NUM_BATCH_REQUESTS;

    private Builder(FunctionType functionType, URI endpoint) {
      this.functionType = Objects.requireNonNull(functionType);
      this.endpoint = Objects.requireNonNull(endpoint);
    }

    public Builder withState(String stateName) {
      this.states.add(stateName);
      return this;
    }

    public Builder withMaxRequestDuration(Duration duration) {
      this.maxRequestDuration = Objects.requireNonNull(duration);
      return this;
    }

    public Builder withMaxNumBatchRequests(int maxNumBatchRequests) {
      this.maxNumBatchRequests = maxNumBatchRequests;
      return this;
    }

    public HttpFunctionSpec build() {
      return new HttpFunctionSpec(
          functionType, endpoint, states, maxRequestDuration, maxNumBatchRequests);
    }
  }
}
