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

package org.apache.flink.statefun.sdk.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;

/**
 * A {@link PersistedStateRegistry} can be used to register persisted state, such as a {@link
 * PersistedValue} or {@link PersistedTable}, etc. All state that is registered via this registry is
 * persisted and maintained by the system for fault-tolerance.
 *
 * <p>Created state registries must be bound to the system by using the {@link Persisted}
 * annotation. Please see the class-level Javadoc of {@link StatefulFunction} for an example on how
 * to do that.
 *
 * @see StatefulFunction
 */
public final class PersistedStateRegistry {

  private final Map<String, Object> registeredStates = new HashMap<>();

  private StateBinder stateBinder;

  public PersistedStateRegistry() {
    this.stateBinder = new NonFaultTolerantStateBinder();
  }

  /**
   * Registers a {@link PersistedValue}. The registration throws if a registered state already
   * exists for the given name.
   *
   * @param value the value to register.
   * @param <T> the type of the value.
   * @return the registered value, now bound to the system for fault-tolerance.
   * @throws IllegalStateException if a previous registration already exists for the given state
   *     name.
   */
  public <T> PersistedValue<T> registerValue(PersistedValue<T> value) {
    registerOrThrowIfPresent(value.name(), value);
    stateBinder.bindValue(value);
    return value;
  }

  /**
   * Registers a {@link PersistedTable}. The registration throws if a registered state already
   * exists for the given name.
   *
   * @param table the table to register.
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @return the registered table, now bound to the system for fault-tolerance.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <K, V> PersistedTable<K, V> registerTable(PersistedTable<K, V> table) {
    registerOrThrowIfPresent(table.name(), table);
    stateBinder.bindTable(table);
    return table;
  }

  /**
   * Registers a {@link PersistedAppendingBuffer}. The registration throws if a registered state
   * already exists for the given name.
   *
   * @param appendingBuffer the appending buffer to register.
   * @param <E> the type of the buffer elements.
   * @return the registered buffer, now bound to the system for fault-tolerance.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <E> PersistedAppendingBuffer<E> registerAppendingBuffer(
      PersistedAppendingBuffer<E> appendingBuffer) {
    registerOrThrowIfPresent(appendingBuffer.name(), appendingBuffer);
    stateBinder.bindAppendingBuffer(appendingBuffer);
    return appendingBuffer;
  }

  /**
   * Binds this state registry to a given function. All existing registered state in this registry
   * will also be bound to the system.
   *
   * @param stateBinder the new fault-tolerant state binder to use.
   * @throws IllegalStateException if the registry was attempted to be bound more than once.
   */
  @ForRuntime
  void bind(StateBinder stateBinder) {
    this.stateBinder = Objects.requireNonNull(stateBinder);

    registeredStates.values().forEach(stateBinder::bind);
  }

  private void registerOrThrowIfPresent(String stateName, Object stateObject) {
    final Object previous = registeredStates.get(stateName);
    if (previous != null) {
      throw new IllegalStateException(
          "A state with name: \""
              + stateName
              + "\" was already registered. Previous: "
              + previous
              + ", now: "
              + stateObject);
    }
    registeredStates.put(stateName, stateObject);
  }

  private static final class NonFaultTolerantStateBinder extends StateBinder {
    @Override
    public void bindValue(PersistedValue<?> persistedValue) {}

    @Override
    public void bindTable(PersistedTable<?, ?> persistedTable) {}

    @Override
    public void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer) {}
  }
}
