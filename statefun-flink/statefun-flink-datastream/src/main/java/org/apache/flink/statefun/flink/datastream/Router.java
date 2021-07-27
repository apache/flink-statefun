/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.datastream;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * A {@link org.apache.flink.statefun.sdk.io.Router} routes messages from ingresses to individual
 * {@link StatefulFunction}s.
 *
 * <p>Implementations should be stateless, as any state in routers are not persisted by the system.
 *
 * @param <InT> the type of messages being routed.
 */
@FunctionalInterface
public interface Router<InT> extends Function {

  /**
   * Routes a given message to downstream {@code StatefulFunction} instance. The target address may
   * either be remote or embedded.
   *
   * @param message The outgoing message.
   * @return The destination address.
   */
  Address route(InT message);
}
