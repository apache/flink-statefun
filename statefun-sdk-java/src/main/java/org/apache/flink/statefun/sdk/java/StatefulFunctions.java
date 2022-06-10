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
package org.apache.flink.statefun.sdk.java;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.statefun.sdk.java.handler.ConcurrentRequestReplyHandler;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

/**
 * A registry for multiple {@link StatefulFunction}s. A {@link RequestReplyHandler} can be created
 * from the registry that understands how to dispatch invocation requests to the registered
 * functions as well as encode side-effects (e.g., sending messages to other functions or updating
 * values in storage) as the response.
 */
public class StatefulFunctions {
  private final Map<TypeName, StatefulFunctionSpec> specs = new HashMap<>();

  /**
   * Registers a {@link StatefulFunctionSpec} builder, which will be used to build the function
   * spec.
   *
   * @param builder a builder for the function spec to register.
   * @throws IllegalArgumentException if multiple {@link StatefulFunctionSpec} under the same {@link
   *     TypeName} have been registered.
   */
  public StatefulFunctions withStatefulFunction(StatefulFunctionSpec.Builder builder) {
    return withStatefulFunction(builder.build());
  }

  /**
   * Registers a {@link StatefulFunctionSpec}.
   *
   * @param spec the function spec to register.
   * @throws IllegalArgumentException if multiple {@link StatefulFunctionSpec} under the same {@link
   *     TypeName} have been registered.
   */
  public StatefulFunctions withStatefulFunction(StatefulFunctionSpec spec) {
    if (specs.put(spec.typeName(), spec) != null) {
      throw new IllegalArgumentException(
          "Attempted to register more than one StatefulFunctionSpec for the function typename: "
              + spec.typeName());
    }
    return this;
  }

  /** @return The registered {@link StatefulFunctionSpec}s. */
  public Map<TypeName, StatefulFunctionSpec> functionSpecs() {
    return specs;
  }

  /**
   * Creates a {@link RequestReplyHandler} that understands how to dispatch invocation requests to
   * the registered functions as well as encode side-effects (e.g., sending messages to other
   * functions or updating values in storage) as the response.
   *
   * @return a {@link RequestReplyHandler} for the registered functions.
   */
  public RequestReplyHandler requestReplyHandler() {
    return new ConcurrentRequestReplyHandler(specs);
  }
}
