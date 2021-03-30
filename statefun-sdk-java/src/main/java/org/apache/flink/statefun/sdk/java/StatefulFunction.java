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

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.message.Message;

/**
 * A {@link StatefulFunction} is a user-defined function that can be invoked with a given input.
 * This is the primitive building block for a Stateful Functions application.
 *
 * <h2>Concept</h2>
 *
 * <p>Each individual {@code StatefulFunction} is an uniquely invokable "instance" of a registered
 * {@link StatefulFunctionSpec}. Each instance is identified by an {@link Address}, representing the
 * function's unique id (a string) within its type. From a user's perspective, it would seem as if
 * for each unique function id, there exists a stateful instance of the function that is always
 * available to be invoked within a Stateful Functions application.
 *
 * <h2>Invoking a {@code StatefulFunction}</h2>
 *
 * <p>An individual {@code StatefulFunction} can be invoked with arbitrary input from any another
 * {@code StatefulFunction} (including itself), or routed from ingresses. To invoke a {@code
 * StatefulFunction}, the caller simply needs to know the {@code Address} of the target function.
 *
 * <p>As a result of invoking a {@code StatefulFunction}, the function may continue to invoke other
 * functions, access persisted values, or send messages to egresses.
 *
 * <h2>Persistent State</h2>
 *
 * <p>Each individual {@code StatefulFunction} may have persistent values written to storage that is
 * maintained by the system, providing consistent exactly-once and fault-tolerant guarantees. Please
 * see Javadocs in {@link ValueSpec} and {@link AddressScopedStorage} for an overview of how to
 * register persistent values and access the storage.
 *
 * @see Address
 * @see StatefulFunctionSpec
 * @see ValueSpec
 * @see AddressScopedStorage
 */
public interface StatefulFunction {

  /**
   * Applies an input message to this function.
   *
   * @param context the {@link Context} of the current invocation.
   * @param argument the input message.
   */
  CompletableFuture<Void> apply(Context context, Message argument) throws Throwable;
}
