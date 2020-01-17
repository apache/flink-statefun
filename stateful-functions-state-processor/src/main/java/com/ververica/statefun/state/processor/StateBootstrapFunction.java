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

package com.ververica.statefun.state.processor;

import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.StatefulFunction;

/**
 * A {@link StateBootstrapFunction} defines how to bootstrap state for a {@link StatefulFunction}
 * instance with a given input.
 *
 * <p>Each {@code StateBootstrapFunction} instance directly corresponds to a {@code
 * StatefulFunction} instance. Likewise, each instance is uniquely identified by an {@link Address},
 * represented by the type and id of the function being bootstrapped. Any state that is persisted by
 * a {@code StateBootstrapFunction} instance will be available to the corresponding live {@code
 * StatefulFunction} instance having the same address.
 *
 * <p>For example, consider the following state bootstrap function:
 *
 * <pre>{@code
 * public class MyStateBootstrapFunction implements StateBootstrapFunction {
 *
 *     @Persisted
 *     private PersistedValue<MyState> state = PersistedValue.of("my-state", MyState.class);
 *
 *     @Override
 *     public void bootstrap(Context context, Object input) {
 *         state.set(extractStateFromInput(input));
 *     }
 * }
 * }</pre>
 *
 * <p>Assume that this bootstrap function was provided for function type {@literal MyFunctionType},
 * and the id of the bootstrap function instance was {@literal id-13}. The function writes persisted
 * state of name {@literal my-state} using the given bootstrap data. After restoring a Stateful
 * Functions application from the savepoint generated using this bootstrap function, the stateful
 * function instance with address {@literal (MyFunctionType, id-13)} will already have state values
 * available under state name {@literal my-state}.
 */
public interface StateBootstrapFunction {

  /**
   * Bootstraps state for this function with the given bootstrap data.
   *
   * @param context context for the current bootstrap invocation. The provided context instance
   *     should not be used outside the scope of the current invocation.
   * @param bootstrapData input to be used for bootstrapping state.
   */
  void bootstrap(Context context, Object bootstrapData);
}
