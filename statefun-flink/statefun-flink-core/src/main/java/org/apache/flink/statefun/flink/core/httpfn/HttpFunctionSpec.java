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
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec;
import org.apache.flink.statefun.sdk.FunctionType;

public final class HttpFunctionSpec implements FunctionSpec, Serializable {

  private static final long serialVersionUID = 1;

  private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration DEFAULT_HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_HTTP_READ_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_HTTP_WRITE_TIMEOUT = Duration.ofSeconds(10);
  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;

  private final FunctionType functionType;
  private final URI endpoint;
  private final List<StateSpec> states;
  private final Duration maxRequestDuration;
  private final Duration connectTimeout;
  private final Duration readTimeout;
  private final Duration writeTimeout;
  private final int maxNumBatchRequests;

  private HttpFunctionSpec(
      FunctionType functionType,
      URI endpoint,
      List<StateSpec> states,
      Duration maxRequestDuration,
      Duration connectTimeout,
      Duration readTimeout,
      Duration writeTimeout,
      int maxNumBatchRequests) {
    this.functionType = Objects.requireNonNull(functionType);
    this.endpoint = Objects.requireNonNull(endpoint);
    this.states = Objects.requireNonNull(states);
    this.maxRequestDuration = Objects.requireNonNull(maxRequestDuration);
    this.connectTimeout = Objects.requireNonNull(connectTimeout);
    this.readTimeout = Objects.requireNonNull(readTimeout);
    this.writeTimeout = Objects.requireNonNull(writeTimeout);
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

  public List<StateSpec> states() {
    return states;
  }

  public Duration maxRequestDuration() {
    return maxRequestDuration;
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public Duration readTimeout() {
    return readTimeout;
  }

  public Duration writeTimeout() {
    return writeTimeout;
  }

  public int maxNumBatchRequests() {
    return maxNumBatchRequests;
  }

  public static final class Builder {

    private final FunctionType functionType;
    private final URI endpoint;

    private final List<StateSpec> states = new ArrayList<>();
    private Duration maxRequestDuration = DEFAULT_HTTP_TIMEOUT;
    private Duration connectTimeout = DEFAULT_HTTP_CONNECT_TIMEOUT;
    private Duration readTimeout = DEFAULT_HTTP_READ_TIMEOUT;
    private Duration writeTimeout = DEFAULT_HTTP_WRITE_TIMEOUT;
    private int maxNumBatchRequests = DEFAULT_MAX_NUM_BATCH_REQUESTS;

    private Builder(FunctionType functionType, URI endpoint) {
      this.functionType = Objects.requireNonNull(functionType);
      this.endpoint = Objects.requireNonNull(endpoint);
    }

    public Builder withState(StateSpec stateSpec) {
      this.states.add(stateSpec);
      return this;
    }

    public Builder withMaxRequestDuration(Duration duration) {
      this.maxRequestDuration = requireNonZeroDuration(duration);
      return this;
    }

    public Builder withConnectTimeoutDuration(Duration duration) {
      this.connectTimeout = requireNonZeroDuration(duration);
      return this;
    }

    public Builder withReadTimeoutDuration(Duration duration) {
      this.readTimeout = requireNonZeroDuration(duration);
      return this;
    }

    public Builder withWriteTimeoutDuration(Duration duration) {
      this.writeTimeout = requireNonZeroDuration(duration);
      return this;
    }

    public Builder withMaxNumBatchRequests(int maxNumBatchRequests) {
      this.maxNumBatchRequests = maxNumBatchRequests;
      return this;
    }

    public HttpFunctionSpec build() {
      validateTimeouts();

      return new HttpFunctionSpec(
          functionType,
          endpoint,
          states,
          maxRequestDuration,
          connectTimeout,
          readTimeout,
          writeTimeout,
          maxNumBatchRequests);
    }

    private Duration requireNonZeroDuration(Duration duration) {
      Objects.requireNonNull(duration);
      if (duration.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("Timeout durations must be larger than 0.");
      }

      return duration;
    }

    private void validateTimeouts() {
      if (connectTimeout.compareTo(maxRequestDuration) > 0) {
        throw new IllegalArgumentException(
            "Connect timeout cannot be larger than request timeout.");
      }

      if (readTimeout.compareTo(maxRequestDuration) > 0) {
        throw new IllegalArgumentException("Read timeout cannot be larger than request timeout.");
      }

      if (writeTimeout.compareTo(maxRequestDuration) > 0) {
        throw new IllegalArgumentException("Write timeout cannot be larger than request timeout.");
      }
    }
  }
}
