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
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionEndpointSpec;

public final class HttpFunctionEndpointSpec implements FunctionEndpointSpec, Serializable {

  private static final long serialVersionUID = 1;

  private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration DEFAULT_HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_HTTP_READ_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_HTTP_WRITE_TIMEOUT = Duration.ofSeconds(10);
  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;

  private final Target target;
  private final UrlPathTemplate urlPathTemplate;

  private final Duration maxRequestDuration;
  private final Duration connectTimeout;
  private final Duration readTimeout;
  private final Duration writeTimeout;
  private final int maxNumBatchRequests;

  public static Builder builder(Target target, UrlPathTemplate urlPathTemplate) {
    return new Builder(target, urlPathTemplate);
  }

  private HttpFunctionEndpointSpec(
      Target target,
      UrlPathTemplate urlPathTemplate,
      Duration maxRequestDuration,
      Duration connectTimeout,
      Duration readTimeout,
      Duration writeTimeout,
      int maxNumBatchRequests) {
    this.target = target;
    this.urlPathTemplate = urlPathTemplate;
    this.maxRequestDuration = maxRequestDuration;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.writeTimeout = writeTimeout;
    this.maxNumBatchRequests = maxNumBatchRequests;
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

    private final Target target;
    private final UrlPathTemplate urlPathTemplate;

    private Duration maxRequestDuration = DEFAULT_HTTP_TIMEOUT;
    private Duration connectTimeout = DEFAULT_HTTP_CONNECT_TIMEOUT;
    private Duration readTimeout = DEFAULT_HTTP_READ_TIMEOUT;
    private Duration writeTimeout = DEFAULT_HTTP_WRITE_TIMEOUT;
    private int maxNumBatchRequests = DEFAULT_MAX_NUM_BATCH_REQUESTS;

    private Builder(Target target, UrlPathTemplate urlPathTemplate) {
      this.target = Objects.requireNonNull(target);
      this.urlPathTemplate = Objects.requireNonNull(urlPathTemplate);
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

    public HttpFunctionEndpointSpec build() {
      validateTimeouts();

      return new HttpFunctionEndpointSpec(
          target,
          urlPathTemplate,
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
