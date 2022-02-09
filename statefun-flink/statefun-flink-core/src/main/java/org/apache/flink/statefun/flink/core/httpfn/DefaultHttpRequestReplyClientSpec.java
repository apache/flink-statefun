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

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public final class DefaultHttpRequestReplyClientSpec {

  @JsonProperty("timeouts")
  private Timeouts timeouts = new Timeouts();

  @JsonSetter("timeouts")
  public void setTimeouts(Timeouts timeouts) {
    validateTimeouts(
        timeouts.callTimeout, timeouts.connectTimeout, timeouts.readTimeout, timeouts.writeTimeout);
    this.timeouts = timeouts;
  }

  public Timeouts getTimeouts() {
    return timeouts;
  }

  public ObjectNode toJson(ObjectMapper objectMapper) {
    return objectMapper.valueToTree(this);
  }

  static DefaultHttpRequestReplyClientSpec fromJson(ObjectMapper objectMapper, JsonNode jsonNode)
      throws JsonProcessingException {
    return objectMapper.treeToValue(jsonNode, DefaultHttpRequestReplyClientSpec.class);
  }

  private static void validateTimeouts(
      Duration callTimeout, Duration connectTimeout, Duration readTimeout, Duration writeTimeout) {

    if (connectTimeout.compareTo(callTimeout) > 0) {
      throw new IllegalArgumentException("Connect timeout cannot be larger than request timeout.");
    }

    if (readTimeout.compareTo(callTimeout) > 0) {
      throw new IllegalArgumentException("Read timeout cannot be larger than request timeout.");
    }

    if (writeTimeout.compareTo(callTimeout) > 0) {
      throw new IllegalArgumentException("Write timeout cannot be larger than request timeout.");
    }
  }

  public static final class Timeouts {

    // default spec values
    @VisibleForTesting public static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);

    @VisibleForTesting
    public static final Duration DEFAULT_HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    @VisibleForTesting
    public static final Duration DEFAULT_HTTP_READ_TIMEOUT = Duration.ofSeconds(10);

    @VisibleForTesting
    public static final Duration DEFAULT_HTTP_WRITE_TIMEOUT = Duration.ofSeconds(10);

    // spec values
    private Duration callTimeout = DEFAULT_HTTP_TIMEOUT;
    private Duration connectTimeout = DEFAULT_HTTP_CONNECT_TIMEOUT;
    private Duration readTimeout = DEFAULT_HTTP_READ_TIMEOUT;
    private Duration writeTimeout = DEFAULT_HTTP_WRITE_TIMEOUT;

    @JsonSetter("call")
    public void setCallTimeout(Duration callTimeout) {
      this.callTimeout = requireNonZeroDuration(callTimeout);
    }

    @JsonSetter("connect")
    public void setConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = requireNonZeroDuration(connectTimeout);
    }

    @JsonSetter("read")
    public void setReadTimeout(Duration readTimeout) {
      this.readTimeout = requireNonZeroDuration(readTimeout);
    }

    @JsonSetter("write")
    public void setWriteTimeout(Duration writeTimeout) {
      this.writeTimeout = requireNonZeroDuration(writeTimeout);
    }

    public Duration getCallTimeout() {
      return callTimeout;
    }

    public Duration getConnectTimeout() {
      return connectTimeout;
    }

    public Duration getReadTimeout() {
      return readTimeout;
    }

    public Duration getWriteTimeout() {
      return writeTimeout;
    }

    private static Duration requireNonZeroDuration(Duration duration) {
      Objects.requireNonNull(duration);
      if (duration.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("Timeout durations must be larger than 0.");
      }

      return duration;
    }
  }
}
