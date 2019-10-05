/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.sdk;

import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.Router;
import com.ververica.statefun.sdk.state.PersistedValue;

/**
 * A {@link StatefulFunction} is a user-defined function that can be invoked with a given input.
 * This is the primitive building block for a Stateful Functions application.
 *
 * <h2>Concept</h2>
 *
 * <p>Each individual {@code StatefulFunction} is an uniquely invokable "instance" of a {@link
 * FunctionType}. Each function is identified by an {@link Address}, representing the function's
 * unique id (a string) within its type. From a user's perspective, it would seem as if for each
 * unique function id, there exists a stateful instance of the function that is always available to
 * be invoked within a Stateful Functions application.
 *
 * <h2>Invoking a {@code StatefulFunction}</h2>
 *
 * <p>An individual {@code StatefulFunction} can be invoked with arbitrary input from any another
 * {@code StatefulFunction} (including itself), or routed from ingresses via a {@link Router}. To
 * invoke a {@code StatefulFunction}, the caller simply needs to know the {@code Address} of the
 * target function.
 *
 * <p>As a result of invoking a {@code StatefulFunction}, the function may continue to invoke other
 * functions, modify its state, or send messages to egresses addressed by an {@link
 * EgressIdentifier}.
 *
 * <h2>State</h2>
 *
 * <p>Each individual {@code StatefulFunction} may have state that is maintained by the system,
 * providing exactly-once guarantees. Below is a code example of how to register and access state in
 * functions:
 *
 * <pre>{@code
 * public class MyFunction implements StatefulFunction {
 *
 *     @Persisted
 *     PersistedValue<Integer> intState = PersistedValue.of("state-name", Integer.class);
 *
 *     @Override
 *     public void invoke(Context context, Object input) {
 *         Integer stateValue = intState.get();
 *         //...
 *         intState.set(1108);
 *         // send messages using context
 *     }
 * }
 * }</pre>
 *
 * @see Address
 * @see FunctionType
 * @see Persisted
 * @see PersistedValue
 */
public interface StatefulFunction {

  /**
   * Invokes this function with a given input.
   *
   * @param context context for the current invocation. The provided context instance should not be
   *     used outside the scope of the current invocation.
   * @param input input for the current invocation.
   */
  void invoke(Context context, Object input);
}
