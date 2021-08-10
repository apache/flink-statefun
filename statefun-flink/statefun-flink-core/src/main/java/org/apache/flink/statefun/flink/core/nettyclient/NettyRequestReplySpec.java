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

import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public final class NettyRequestReplySpec {

  @JsonProperty("call")
  public Duration callTimeout = Duration.ofMinutes(2);

  @JsonProperty("connect")
  public Duration connectTimeout = Duration.ofSeconds(20);

  @JsonProperty("pool_ttl")
  public Duration pooledConnectionTTL = Duration.ofSeconds(15);

  @JsonProperty("pool_size")
  public int connectionPoolMaxSize = 1024;

  @JsonProperty("payload_max_bytes")
  public int maxRequestOrResponseSizeInBytes = 32 * 1048576;
}
