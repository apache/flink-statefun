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
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.FunctionType;
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

  /**
   * The type of the function that this registry is bound to. This is {@code NULL} if this registry
   * is not bounded.
   */
  @Nullable private FunctionType functionType;

  public PersistedStateRegistry() {
    this.stateBinder = new NonFaultTolerantStateBinder();
  }

  /**
   * Registers a {@link PersistedValue}, given a state name and the type of the values. If a
   * registered value already exists for the given name, the previous persisted value is returned.
   *
   * @param name the state name to register with.
   * @param type the type of the value.
   * @param <T> the type of the value.
   * @return the registered value, or the previous registered value if a registration for the state
   *     name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedValue}.
   */
  public <T> PersistedValue<T> registerValue(String name, Class<T> type) {
    return registerValue(name, type, Expiration.none());
  }

  /**
   * Registers a {@link PersistedValue}, given a state name and the type of the values. If a
   * registered value already exists for the given name, the previous persisted value is returned.
   *
   * @param name the state name to register with.
   * @param type the type of the value.
   * @param expiration expiration configuration for the registered state.
   * @param <T> the type of the value.
   * @return the registered value, or the previous registered value if a registration for the state
   *     name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedValue}.
   */
  public <T> PersistedValue<T> registerValue(String name, Class<T> type, Expiration expiration) {
    return getStateOrCreateIfAbsent(
        PersistedValue.class, name, stateName -> createValue(stateName, type, expiration));
  }

  /**
   * Registers a {@link PersistedTable}, given a state name and the type of the keys and values of
   * the table. If a registered value already exists for the given name, the previous persisted
   * table is returned.
   *
   * @param name the state name to register with.
   * @param keyType the type of the keys.
   * @param valueType the type of the values.
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @return the registered table, or the previous registered table if a registration for the state
   *     name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedTable}.
   */
  public <K, V> PersistedTable<K, V> registerTable(
      String name, Class<K> keyType, Class<V> valueType) {
    return registerTable(name, keyType, valueType, Expiration.none());
  }

  /**
   * Registers a {@link PersistedTable}, given a state name and the type of the keys and values of
   * the table. If a registered value already exists for the given name, the previous persisted
   * table is returned.
   *
   * @param name the state name to register with.
   * @param keyType the type of the keys.
   * @param valueType the type of the values.
   * @param expiration expiration configuration for the registered state.
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @return the registered table, or the previous registered table if a registration for the state
   *     name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedTable}.
   */
  public <K, V> PersistedTable<K, V> registerTable(
      String name, Class<K> keyType, Class<V> valueType, Expiration expiration) {
    return getStateOrCreateIfAbsent(
        PersistedTable.class,
        name,
        stateName -> createTable(stateName, keyType, valueType, expiration));
  }

  /**
   * Registers a {@link PersistedAppendingBuffer}, given a state name and the type of the buffer
   * elements. If a registered buffer already exists for the given name, the previous persisted
   * buffer is returned.
   *
   * @param name the state name to register with.
   * @param elementType the type of the buffer elements.
   * @param <E> the type of the buffer elements.
   * @return the registered buffer, or the previous registered buffer if a registration for the
   *     state name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedAppendingBuffer}.
   */
  public <E> PersistedAppendingBuffer<E> registerAppendingBuffer(
      String name, Class<E> elementType) {
    return registerAppendingBuffer(name, elementType, Expiration.none());
  }

  /**
   * Registers a {@link PersistedAppendingBuffer}, given a state name and the type of the buffer
   * elements. If a registered buffer already exists for the given name, the previous persisted
   * buffer is returned.
   *
   * @param name the state name to register with.
   * @param elementType the type of the buffer elements.
   * @param expiration expiration configuration for the registered state.
   * @param <E> the type of the buffer elements.
   * @return the registered buffer, or the previous registered buffer if a registration for the
   *     state name already exists.
   * @throws IllegalStateException if a previous registration exists for the given state name, but
   *     it wasn't registered as a {@link PersistedAppendingBuffer}.
   */
  public <E> PersistedAppendingBuffer<E> registerAppendingBuffer(
      String name, Class<E> elementType, Expiration expiration) {
    return getStateOrCreateIfAbsent(
        PersistedAppendingBuffer.class,
        name,
        stateName -> createAppendingBuffer(stateName, elementType, expiration));
  }

  /**
   * Binds this state registry to a given function. All existing registered state in this registry
   * will also be bound to the system.
   *
   * @param stateBinder the new fault-tolerant state binder to use.
   * @param functionType the type of the function that this registry is being bound to.
   * @throws IllegalStateException if the registry was attempted to be bound more than once.
   */
  @ForRuntime
  void bind(StateBinder stateBinder, FunctionType functionType) {
    if (this.functionType != null) {
      throw new IllegalStateException(
          "This registry was already bound to function type: "
              + this.functionType
              + ", attempting to rebind to function type: "
              + functionType);
    }

    this.stateBinder = Objects.requireNonNull(stateBinder);
    this.functionType = Objects.requireNonNull(functionType);

    registeredStates.values().forEach(state -> stateBinder.bind(state, functionType));
  }

  private <T> PersistedValue<T> createValue(String name, Class<T> type, Expiration expiration) {
    final PersistedValue<T> value = PersistedValue.of(name, type, expiration);
    stateBinder.bindValue(value, functionType);
    return value;
  }

  private <K, V> PersistedTable<K, V> createTable(
      String name, Class<K> keyType, Class<V> valueType, Expiration expiration) {
    final PersistedTable<K, V> table = PersistedTable.of(name, keyType, valueType, expiration);
    stateBinder.bindTable(table, functionType);
    return table;
  }

  private <E> PersistedAppendingBuffer<E> createAppendingBuffer(
      String name, Class<E> elementType, Expiration expiration) {
    final PersistedAppendingBuffer<E> buffer =
        PersistedAppendingBuffer.of(name, elementType, expiration);
    stateBinder.bindAppendingBuffer(buffer, functionType);
    return buffer;
  }

  @SuppressWarnings("unchecked")
  private <ST> ST getStateOrCreateIfAbsent(
      Class<?> statePrimitiveType, String name, Function<String, ST> createFunction) {
    final ST state = (ST) registeredStates.computeIfAbsent(name, createFunction::apply);
    if (state.getClass() != statePrimitiveType) {
      throw new IllegalStateException(
          "Unexpected state primitive type. The state was registered with type: "
              + state.getClass()
              + ", but was attempting to access it again as type: "
              + statePrimitiveType);
    }
    return state;
  }

  private static final class NonFaultTolerantStateBinder extends StateBinder {
    @Override
    public void bindValue(PersistedValue<?> persistedValue, FunctionType functionType) {}

    @Override
    public void bindTable(PersistedTable<?, ?> persistedTable, FunctionType functionType) {}

    @Override
    public void bindAppendingBuffer(
        PersistedAppendingBuffer<?> persistedAppendingBuffer, FunctionType functionType) {}
  }
}
