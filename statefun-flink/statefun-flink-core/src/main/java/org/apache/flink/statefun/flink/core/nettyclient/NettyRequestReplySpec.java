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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSetter;

public final class NettyRequestReplySpec {

  // property names in the spec
  public static final String CALL_TIMEOUT_PROPERTY = "call";
  public static final String CONNECT_TIMEOUT_PROPERTY = "connect";
  public static final String POOLED_CONNECTION_TTL_PROPERTY = "pool_ttl";
  public static final String CONNECTION_POOL_MAX_SIZE_PROPERTY = "pool_size";
  public static final String MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES_PROPERTY = "payload_max_bytes";
  public static final String TRUST_CA_CERTS_PROPERTY = "trust_cacerts";
  public static final String CLIENT_CERT_PROPERTY = "client_cert";
  public static final String CLIENT_KEY_PROPERTY = "client_key";
  public static final String CLIENT_KEY_PASSWORD_PROPERTY = "client_key_password";
  public static final String TIMEOUTS_PROPERTY = "timeouts";

  // spec default values
  @VisibleForTesting public static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofMinutes(2);
  @VisibleForTesting public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(20);

  @VisibleForTesting
  public static final Duration DEFAULT_POOLED_CONNECTION_TTL = Duration.ofSeconds(15);

  @VisibleForTesting public static final int DEFAULT_CONNECTION_POOL_MAX_SIZE = 1024;

  @VisibleForTesting
  public static final int DEFAULT_MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES = 32 * 1048576;

  // spec values
  public final Duration callTimeout;
  public final Duration connectTimeout;
  public final Duration pooledConnectionTTL;
  public final int connectionPoolMaxSize;
  public final int maxRequestOrResponseSizeInBytes;
  private final String trustedCaCerts;
  private final String clientCerts;
  private final String clientKey;
  private final String clientKeyPassword;

  public NettyRequestReplySpec(
      @JsonProperty(CALL_TIMEOUT_PROPERTY) Duration callTimeout,
      @JsonProperty(CONNECT_TIMEOUT_PROPERTY) Duration connectTimeout,
      @JsonProperty(POOLED_CONNECTION_TTL_PROPERTY) Duration pooledConnectionTTL,
      @JsonProperty(CONNECTION_POOL_MAX_SIZE_PROPERTY) Integer connectionPoolMaxSize,
      @JsonProperty(MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES_PROPERTY)
          Integer maxRequestOrResponseSizeInBytes,
      @JsonProperty(TRUST_CA_CERTS_PROPERTY) String trustedCaCerts,
      @JsonProperty(CLIENT_CERT_PROPERTY) String clientCerts,
      @JsonProperty(CLIENT_KEY_PROPERTY) String clientKey,
      @JsonProperty(CLIENT_KEY_PASSWORD_PROPERTY) String clientKeyPassword,
      @JsonProperty(TIMEOUTS_PROPERTY) Timeouts timeouts) {
    this.trustedCaCerts = trustedCaCerts;
    this.clientCerts = clientCerts;
    this.clientKey = clientKey;
    this.clientKeyPassword = clientKeyPassword;
    this.callTimeout =
        firstPresentOrDefault(
            ofNullable(timeouts).map(Timeouts::getCallTimeout),
            ofNullable(callTimeout),
            () -> DEFAULT_CALL_TIMEOUT);

    this.connectTimeout =
        firstPresentOrDefault(
            ofNullable(timeouts).map(Timeouts::getConnectTimeout),
            ofNullable(connectTimeout),
            () -> DEFAULT_CONNECT_TIMEOUT);
    this.pooledConnectionTTL =
        ofNullable(pooledConnectionTTL).orElse(DEFAULT_POOLED_CONNECTION_TTL);
    this.connectionPoolMaxSize =
        ofNullable(connectionPoolMaxSize).orElse(DEFAULT_CONNECTION_POOL_MAX_SIZE);
    this.maxRequestOrResponseSizeInBytes =
        ofNullable(maxRequestOrResponseSizeInBytes)
            .orElse(DEFAULT_MAX_REQUEST_OR_RESPONSE_SIZE_IN_BYTES);
  }

  public Optional<String> getTrustedCaCerts() {
    return Optional.ofNullable(trustedCaCerts);
  }

  public Optional<String> getClientCerts() {
    return Optional.ofNullable(clientCerts);
  }

  public Optional<String> getClientKey() {
    return Optional.ofNullable(clientKey);
  }

  public Optional<String> getClientKeyPassword() {
    return Optional.ofNullable(clientKeyPassword);
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
