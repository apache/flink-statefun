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
import java.util.List;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec;
import org.apache.flink.statefun.sdk.FunctionType;

public final class HttpFunctionSpec implements FunctionSpec {
  private final FunctionType functionType;
  private final URI endpoint;
  private final String unixDomainSocket;
  private final List<String> states;
  private final Duration maxRequestDuration;
  private final int maxNumBatchRequests;

  public HttpFunctionSpec(
      FunctionType functionType,
      URI endpoint,
      String unixDomainSocket,
      List<String> states,
      Duration maxRequestDuration,
      int maxNumBatchRequests) {
    this.functionType = Objects.requireNonNull(functionType);
    this.endpoint = Objects.requireNonNull(endpoint);
    this.unixDomainSocket = unixDomainSocket;
    this.states = Objects.requireNonNull(states);
    this.maxRequestDuration = Objects.requireNonNull(maxRequestDuration);
    this.maxNumBatchRequests = maxNumBatchRequests;
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

  public String unixDomainSocket() {
    return unixDomainSocket;
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
}
