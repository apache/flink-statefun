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

package org.apache.flink.statefun.sdk.java.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

/**
 * Utility for pairing registered {@link ValueSpec}s with values provided by the protocol's {@link
 * ToFunction} message.
 */
@Internal
public final class StateValueContexts {

  public static final class StateValueContext<T> {
    private final ValueSpec<T> spec;
    private final ToFunction.PersistedValue protocolValue;

    StateValueContext(ValueSpec<T> spec, ToFunction.PersistedValue protocolValue) {
      this.spec = Objects.requireNonNull(spec);
      this.protocolValue = Objects.requireNonNull(protocolValue);
    }

    public ValueSpec<T> spec() {
      return spec;
    }

    public ToFunction.PersistedValue protocolValue() {
      return protocolValue;
    }
  }

  public static final class ResolutionResult {
    private final List<StateValueContext<?>> resolved;
    private final List<ValueSpec<?>> missingValues;

    private ResolutionResult(
        List<StateValueContext<?>> resolved, List<ValueSpec<?>> missingValues) {
      this.resolved = resolved;
      this.missingValues = missingValues;
    }

    public boolean hasMissingValues() {
      return missingValues != null;
    }

    public List<StateValueContext<?>> resolved() {
      return resolved;
    }

    public List<ValueSpec<?>> missingValues() {
      return missingValues;
    }
  }

  /**
   * Tries to resolve any registered states that are known to us by the {@link ValueSpec} with the
   * states provided to us by the runtime.
   */
  public static ResolutionResult resolve(
      Map<String, ValueSpec<?>> registeredSpecs,
      List<ToFunction.PersistedValue> protocolProvidedValues) {

    // holds a set of missing ValueSpec's. a missing ValueSpec is a value spec that was
    // registered by the user but wasn't sent to the SDK by the runtime.
    // this can happen upon an initial request.
    List<ValueSpec<?>> statesWithMissingValue =
        null; // optimize for normal execution, where states aren't missing.

    // holds the StateValueContext that will be used to serialize and deserialize user state.
    final List<StateValueContext<?>> resolvedStateValues = new ArrayList<>(registeredSpecs.size());

    for (ValueSpec<?> spec : registeredSpecs.values()) {
      ToFunction.PersistedValue persistedValue =
          findPersistedValueByName(protocolProvidedValues, spec.name());

      if (persistedValue != null) {
        resolvedStateValues.add(new StateValueContext<>(spec, persistedValue));
      } else {
        // oh no. the runtime doesn't know (yet) about a state that was registered by the user.
        // we need to collect these.
        statesWithMissingValue =
            (statesWithMissingValue != null)
                ? statesWithMissingValue
                : new ArrayList<>(registeredSpecs.size());
        statesWithMissingValue.add(spec);
      }
    }

    if (statesWithMissingValue == null) {
      return new ResolutionResult(resolvedStateValues, null);
    }
    return new ResolutionResult(null, statesWithMissingValue);
  }

  /**
   * finds a {@linkplain org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.PersistedValue}
   * with a given name. The size of the list, in practice is expected to be very small (0-10) items.
   * just use a plain linear search.
   */
  private static ToFunction.PersistedValue findPersistedValueByName(
      List<ToFunction.PersistedValue> protocolProvidedValues, String name) {
    for (ToFunction.PersistedValue value : protocolProvidedValues) {
      if (Objects.equals(value.getStateName(), name)) {
        return value;
      }
    }
    return null;
  }
}
