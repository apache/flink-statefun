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
package org.apache.flink.statefun.sdk.io;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.metrics.Metrics;

/**
 * A {@link Router} routes messages from ingresses to individual {@link StatefulFunction}s.
 *
 * <p>Implementations should be stateless, as any state in routers are not persisted by the system.
 *
 * @param <InT> the type of messages being routed.
 */
public interface Router<InT> {

  /**
   * Routes a given message to downstream {@link StatefulFunction}s. A single message may result in
   * multiple functions being invoked.
   *
   * @param message the message to route.
   * @param downstream used to invoke downstream functions.
   */
  void route(InT message, Downstream<InT> downstream);

  /**
   * Interface for invoking downstream functions.
   *
   * @param <T> the type of messages being routed to downstream functions.
   */
  interface Downstream<T> {

    /**
     * Forwards the message as an input to a downstream function, addressed by a specified {@link
     * Address}.
     *
     * @param to the target function's address.
     * @param message the message being forwarded.
     */
    void forward(Address to, T message);

    /**
     * Forwards the message as an input to a downstream function, addressed by a specified {@link
     * FunctionType} and the functions unique id within its type.
     *
     * @param functionType the target function's type.
     * @param id the target function's unique id.
     * @param message the message being forwarded.
     */
    default void forward(FunctionType functionType, String id, T message) {
      forward(new Address(functionType, id), message);
    }

    /**
     * Returns the metrics that are scoped to this ingress.
     *
     * <p>Every metric defined here will be associated to the ingress, that this router is attached
     * to. For example, if this router is attached to an ingress with an {@code
     * IngressIdentifier("foo", "bar")}, then every metric name {@code M} will appear as
     * "routers.foo.bar.M".
     */
    Metrics metrics();
  }
}
