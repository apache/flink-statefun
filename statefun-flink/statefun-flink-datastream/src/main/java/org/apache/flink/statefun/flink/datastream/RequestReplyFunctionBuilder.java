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

package org.apache.flink.statefun.flink.datastream;

import java.net.URI;
import java.time.Duration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Expiration;

/** A Builder for RequestReply remote function type. */
public class RequestReplyFunctionBuilder {

  /**
   * Create a new builder for a remote function with a given type and an endpoint.
   *
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  public static RequestReplyFunctionBuilder requestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new RequestReplyFunctionBuilder(functionType, endpoint);
  }

  private final HttpFunctionSpec.Builder builder;

  private RequestReplyFunctionBuilder(FunctionType functionType, URI endpoint) {
    this.builder = HttpFunctionSpec.builder(functionType, endpoint);
  }

  /**
   * Declares a remote function state.
   *
   * @param name the name of the state to be used remotely.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withPersistedState(String name) {
    builder.withState(new StateSpec(name, Expiration.none()));
    return this;
  }

  /**
   * Declares a remote function state, with expiration.
   *
   * @param name the name of the state to be used remotely.
   * @param ttlExpiration the expiration mode for which this state might be deleted.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withExpiringState(String name, Expiration ttlExpiration) {
    builder.withState(new StateSpec(name, ttlExpiration));
    return this;
  }

  /**
   * Set a maximum request duration.
   *
   * @param duration the duration after which the request is considered failed.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withMaxRequestDuration(Duration duration) {
    builder.withMaxRequestDuration(duration);
    return this;
  }

  /**
   * Sets the max messages to batch together for a specific address.
   *
   * @param maxNumBatchRequests the maximum number of requests to batch for an address.
   * @return this builder.
   */
  public RequestReplyFunctionBuilder withMaxNumBatchRequests(int maxNumBatchRequests) {
    builder.withMaxNumBatchRequests(maxNumBatchRequests);
    return this;
  }

  @Internal
  HttpFunctionSpec spec() {
    return builder.build();
  }
}
