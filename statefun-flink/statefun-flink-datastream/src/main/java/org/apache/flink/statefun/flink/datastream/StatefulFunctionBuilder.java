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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.sdk.FunctionType;

/** Base class for statefun function builders. */
public abstract class StatefulFunctionBuilder {

  /** The object mapper used to serialize the client spec object. */
  static final ObjectMapper CLIENT_SPEC_OBJ_MAPPER = StateFunObjectMapper.create();

  /**
   * Override to provide the endpoint spec.
   *
   * @return The endpoint spec.
   */
  abstract HttpFunctionEndpointSpec spec();

  /**
   * Creates a function builder using the synchronous HTTP protocol.
   *
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  public static RequestReplyFunctionBuilder requestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new RequestReplyFunctionBuilder(functionType, endpoint);
  }

  /**
   * Creates a function builder using the asynchronous HTTP protocol.
   *
   * @param functionType the function type that is served remotely.
   * @param endpoint the endpoint that serves that remote function.
   * @return a builder.
   */
  public static AsyncRequestReplyFunctionBuilder asyncRequestReplyFunctionBuilder(
      FunctionType functionType, URI endpoint) {
    return new AsyncRequestReplyFunctionBuilder(functionType, endpoint);
  }
}
