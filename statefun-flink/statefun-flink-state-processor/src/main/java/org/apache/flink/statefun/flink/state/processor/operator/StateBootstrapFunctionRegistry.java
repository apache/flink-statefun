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
package org.apache.flink.statefun.flink.state.processor.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.state.PersistedStates;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.flink.core.state.StateBinder;
import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunction;
import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;

/** A registry that handles {@link StateBootstrapFunctionProvider} registrations. */
public final class StateBootstrapFunctionRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  /** State bootstrap function providers registered by the user. */
  private final Map<SerializableFunctionType, StateBootstrapFunctionProvider>
      stateBootstrapFunctionProviders = new HashMap<>();

  /**
   * Registry of instantiated, state-bound bootstrap functions. This is created only after {@link
   * #initialize(State)} is invoked during runtime.
   */
  private transient Map<FunctionType, StateBootstrapFunction> registry;

  /**
   * Registers a {@link StateBootstrapFunctionProvider}.
   *
   * @param functionType the type of the function that is being bootstrapped.
   * @param stateBootstrapFunctionProvider provider of the bootstrap function.
   */
  public void register(
      FunctionType functionType, StateBootstrapFunctionProvider stateBootstrapFunctionProvider) {
    if (isInitialized()) {
      throw new IllegalStateException(
          "Cannot register bootstrap function providers after the registry is initialized.");
    }

    Objects.requireNonNull(functionType);
    Objects.requireNonNull(stateBootstrapFunctionProvider);

    final StateBootstrapFunctionProvider previous =
        stateBootstrapFunctionProviders.put(
            SerializableFunctionType.fromNonSerializable(functionType),
            stateBootstrapFunctionProvider);
    if (previous == null) {
      return;
    }

    throw new IllegalArgumentException(
        String.format(
            "A StateBootstrapFunctionProvider for function type %s was previously defined.",
            functionType));
  }

  /**
   * Returns number of registrations.
   *
   * @return number of registrations.
   */
  public int numRegistrations() {
    return stateBootstrapFunctionProviders.size();
  }

  /**
   * Initializes the registry. This instantiates all registered state bootstrap functions, and binds
   * them with Flink state.
   *
   * @param stateAccessor accessor for Flink state to bind bootstrap functions with.
   */
  void initialize(State stateAccessor) {
    this.registry = new HashMap<>(stateBootstrapFunctionProviders.size());

    final StateBinder stateBinder = new StateBinder(stateAccessor);
    for (Map.Entry<SerializableFunctionType, StateBootstrapFunctionProvider> entry :
        stateBootstrapFunctionProviders.entrySet()) {
      final FunctionType functionType = entry.getKey().toNonSerializable();
      final StateBootstrapFunction bootstrapFunction =
          entry.getValue().bootstrapFunctionOfType(functionType);

      registry.put(functionType, bindState(functionType, bootstrapFunction, stateBinder));
    }
  }

  /** Retrieves the bootstrap function for a given function type. */
  @Nullable
  StateBootstrapFunction getBootstrapFunction(FunctionType functionType) {
    if (!isInitialized()) {
      throw new IllegalStateException("The registry must be initialized first.");
    }

    return registry.get(functionType);
  }

  private static StateBootstrapFunction bindState(
      FunctionType functionType,
      StateBootstrapFunction bootstrapFunction,
      StateBinder stateBinder) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(bootstrapFunction)) {
      PersistedStates.findAndBind(functionType, bootstrapFunction, stateBinder);
      return bootstrapFunction;
    }
  }

  private boolean isInitialized() {
    return registry != null;
  }

  private static final class SerializableFunctionType implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;

    static SerializableFunctionType fromNonSerializable(FunctionType functionType) {
      return new SerializableFunctionType(functionType.namespace(), functionType.name());
    }

    private SerializableFunctionType(String namespace, String name) {
      this.namespace = Objects.requireNonNull(namespace);
      this.name = Objects.requireNonNull(name);
    }

    private FunctionType toNonSerializable() {
      return new FunctionType(namespace, name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SerializableFunctionType that = (SerializableFunctionType) o;
      return namespace.equals(that.namespace) && name.equals(that.name);
    }
  }
}
