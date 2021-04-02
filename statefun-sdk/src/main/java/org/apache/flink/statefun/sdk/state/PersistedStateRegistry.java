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

  private final Map<String, Object> inactiveStates = new HashMap<>();

  private StateBinder stateBinder;

  public PersistedStateRegistry() {
    this.stateBinder = new NonFaultTolerantStateBinder();
  }

  /**
   * Registers a {@link PersistedValue}. If a registered state already exists for the specified name
   * of the value, the registration fails.
   *
   * @param valueState the value state to register.
   * @param <T> the type of the value.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <T> void registerValue(PersistedValue<T> valueState) {
    acceptRegistrationOrThrowIfPresent(valueState.name(), valueState);
  }

  /**
   * Registers a {@link PersistedTable}. If a registered state already exists for the specified name
   * of the table, the registration fails.
   *
   * @param tableState the table state to register.
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <K, V> void registerTable(PersistedTable<K, V> tableState) {
    acceptRegistrationOrThrowIfPresent(tableState.name(), tableState);
  }

  /**
   * Registers a {@link PersistedAppendingBuffer}. If a registered state already exists for the
   * specified name of the table, the registration fails.
   *
   * @param bufferState the appending buffer to register.
   * @param <E> the type of the buffer elements.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <E> void registerAppendingBuffer(PersistedAppendingBuffer<E> bufferState) {
    acceptRegistrationOrThrowIfPresent(bufferState.name(), bufferState);
  }

  public <E> void registerList(PersistedList<E> listState) {
    acceptRegistrationOrThrowIfPresent(listState.name(), listState);
  }

  public boolean checkIfRegistered(String stateName){
    final Object previousRegistration = registeredStates.get(stateName);
    if (previousRegistration != null) {
      return true;
    }
    return false;
  }

  public Object getState(String stateName){
    return registeredStates.get(stateName);
  }

  public void setInactive(String stateName){
    if(registeredStates.get(stateName)!=null){
      inactiveStates.put(stateName, registeredStates.get(stateName));
      registeredStates.remove(stateName);
    }
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
    if (isBound()) {
      throw new IllegalStateException(
          "This registry was already bound to state binder: "
              + this.stateBinder.getClass().getName()
              + ", attempting to rebind to state binder: "
              + stateBinder.getClass().getName());
    }

    this.stateBinder = Objects.requireNonNull(stateBinder);
    registeredStates.values().forEach(stateBinder::bind);
  }

  private boolean isBound() {
    return stateBinder != null && !(stateBinder instanceof NonFaultTolerantStateBinder);
  }

  private void acceptRegistrationOrThrowIfPresent(String stateName, Object newStateObject) {
    final Object previousRegistration = registeredStates.get(stateName);
    if (previousRegistration != null) {
      throw new IllegalStateException(
          String.format(
              "State name '%s' was registered twice; previous registered state object with the same name was a %s, attempting to register a new %s under the same name.",
              stateName, previousRegistration, newStateObject));
    }

    registeredStates.put(stateName, newStateObject);
    stateBinder.bind(newStateObject);
  }

  private static final class NonFaultTolerantStateBinder extends StateBinder {
    @Override
    public void bindValue(PersistedValue<?> persistedValue) {}

    @Override
    public void bindAsyncValue(PersistedAsyncValue<?> persistedValue) {}

    @Override
    public void bindTable(PersistedTable<?, ?> persistedTable) {}

    @Override
    public void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer) {}

    @Override
    public void bindList(PersistedList<?> persistedList) { }
  }
}
