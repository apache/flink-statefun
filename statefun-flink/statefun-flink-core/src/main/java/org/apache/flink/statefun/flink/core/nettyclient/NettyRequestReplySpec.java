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
package org.apache.flink.statefun.flink.core.nettyclient;

import static java.util.Optional.ofNullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSetter;

public final class NettyRequestReplySpec {

  public final Duration callTimeout;

  public final Duration connectTimeout;

  public final Duration pooledConnectionTTL;

  public final int connectionPoolMaxSize;

  public final int maxRequestOrResponseSizeInBytes;

  public NettyRequestReplySpec(
      @JsonProperty("call") Duration callTimeout,
      @JsonProperty("connect") Duration connectTimeout,
      @JsonProperty("pool_ttl") Duration pooledConnectionTTL,
      @JsonProperty("pool_size") Integer connectionPoolMaxSize,
      @JsonProperty("payload_max_bytes") Integer maxRequestOrResponseSizeInBytes,
      @JsonProperty("timeouts") Timeouts timeouts) {
    this.callTimeout =
        firstPresentOrDefault(
            ofNullable(timeouts).map(Timeouts::getCallTimeout),
            ofNullable(callTimeout),
            () -> Duration.ofMinutes(2));

    this.connectTimeout =
        firstPresentOrDefault(
            ofNullable(timeouts).map(Timeouts::getConnectTimeout),
            ofNullable(connectTimeout),
            () -> Duration.ofSeconds(20));
    this.pooledConnectionTTL =
        ofNullable(pooledConnectionTTL).orElseGet(() -> Duration.ofSeconds(15));
    this.connectionPoolMaxSize = ofNullable(connectionPoolMaxSize).orElse(1024);
    this.maxRequestOrResponseSizeInBytes =
        ofNullable(maxRequestOrResponseSizeInBytes).orElse(32 * 1048576);
  }

  /**
   * This is a copy of {@linkplain
   * org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientSpec.Timeouts}, to
   * ease the migration from the {@code DefaultHttpRequestReplyClientFactory}.
   */
  public static final class Timeouts {

    private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration DEFAULT_HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private Duration callTimeout = DEFAULT_HTTP_TIMEOUT;
    private Duration connectTimeout = DEFAULT_HTTP_CONNECT_TIMEOUT;

    @JsonSetter("call")
    public void setCallTimeout(Duration callTimeout) {
      this.callTimeout = requireNonZeroDuration(callTimeout);
    }

    @JsonSetter("connect")
    public void setConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = requireNonZeroDuration(connectTimeout);
    }

    public Duration getCallTimeout() {
      return callTimeout;
    }

    public Duration getConnectTimeout() {
      return connectTimeout;
    }

    private static Duration requireNonZeroDuration(Duration duration) {
      Objects.requireNonNull(duration);
      if (duration.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("Timeout durations must be larger than 0.");
      }
      return duration;
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static <T> T firstPresentOrDefault(Optional<T> a, Optional<T> b, Supplier<T> orElse) {
    return a.orElseGet(() -> b.orElseGet(orElse));
  }
}
