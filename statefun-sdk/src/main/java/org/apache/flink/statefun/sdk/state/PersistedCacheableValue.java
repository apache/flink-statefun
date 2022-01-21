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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link PersistedCacheableValue} is a value registered within {@link StatefulFunction}s and is persisted
 * and maintained by the system for fault-tolerance.
 *
 * <p>Created persisted values must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <T> type of the state.
 */
public class PersistedCacheableValue<T> extends CacheableState{
  private final String name;
  private final Class<T> type;
  private final Expiration expiration;
  private boolean synced;
  protected Accessor<T> accessor;
  protected NonFaultTolerantAccessor<T> cachedAccessor;
  private StateDescriptor descriptor;

  protected PersistedCacheableValue(String name, Class<T> type, Expiration expiration, Accessor<T> accessor) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.expiration = Objects.requireNonNull(expiration);
    this.accessor = Objects.requireNonNull(accessor);
    this.cachedAccessor = new NonFaultTolerantAccessor<>();
    this.synced = true;
    this.descriptor = null;
  }

  /**
   * Creates a {@link PersistedCacheableValue} instance that may be used to access persisted state managed by
   * the system. Access to the persisted value is identified by an unique name and type of the
   * value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param type the type of the state values of this {@code PersistedValue}.
   * @param <T> the type of the state values.
   * @return a {@code PersistedValue} instance.
   */
  public static <T> PersistedCacheableValue<T> of(String name, Class<T> type) {
    return of(name, type, Expiration.none());
  }

  /**
   * Creates a {@link PersistedCacheableValue} instance that may be used to access persisted state managed by
   * the system. Access to the persisted value is identified by an unique name and type of the
   * value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param type the type of the state values of this {@code PersistedValue}.
   * @param expiration state expiration configuration.
   * @param <T> the type of the state values.
   * @return a {@code PersistedValue} instance.
   */
  public static <T> PersistedCacheableValue<T> of(String name, Class<T> type, Expiration expiration) {
    return new PersistedCacheableValue<>(name, type, expiration, new NonFaultTolerantAccessor<>());
  }

  /**
   * Returns the unique name of the persisted value.
   *
   * @return unique name of the persisted value.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the type of the persisted values.
   *
   * @return the type of the persisted values.
   */
  public Class<T> type() {
    return type;
  }

  public Expiration expiration() {
    return expiration;
  }

  /**
   * Returns the persisted value.
   *
   FlinkIntegerValueAccessor  * @return the persisted value.
   */
  public T get() {
    if(synced){
      return accessor.get();
    }
    return cachedAccessor.get();
  }

  /**
   * Updates the persisted value.
   *
   * @param value the new value.
   */
  public void set(T value) {
    cachedAccessor.set(value);
    synced = false;
  }

  /** Clears the persisted value. After being cleared, the value would be {@code null}. */
  public void clear() {
    cachedAccessor.clear();
    synced = false;
  }

  /**
   * Updates the persisted value and returns it, in a single operation.
   *
   * @param update function to process the previous value to obtain the new value.
   * @return the new updated value.
   */
  public T updateAndGet(Function<T, T> update) {
    if(synced){
      T current = accessor.get();
      T updated = update.apply(current);
      cachedAccessor.set(updated);
      synced = false;
      return updated;
    }
    T current = cachedAccessor.get();
    T updated = update.apply(current);
    cachedAccessor.set(updated);
    return updated;
  }

  /**
   * Attempts to get the persisted value. If the current value is {@code null}, then a specified
   * default is returned instead.
   *
   * @param orElse the default value to return if the current value is not present.
   * @return the persisted value, or the provided default if it isn't present.
   */
  public T getOrDefault(T orElse) {
    if(synced){
      T value = accessor.get();
      return value != null ? value : orElse;
    }
    T value = cachedAccessor.get();
    return value != null? value : orElse;
  }

  /**
   * Attempts to get the persisted value. If the current value is {@code null}, then a default value
   * obtained from a specified supplier is returned instead.
   *
   * @param defaultSupplier supplier for a default value to be used if the current value is not
   *     present.
   * @return the persisted value, or a default value if it isn't present.
   */
  public T getOrDefault(Supplier<T> defaultSupplier) {
    if(synced){
      T value = accessor.get();
      return value != null ? value : defaultSupplier.get();
    }
    T value = cachedAccessor.get();
    return value != null ? value : defaultSupplier.get();
  }

  @ForRuntime
  void setAccessor(Accessor<T> newAccessor) {
    Objects.requireNonNull(newAccessor);
    this.accessor = newAccessor;
  }

  public void setDescriptor(StateDescriptor descriptor){
    this.descriptor = descriptor;
  }

  @Override
  public String toString() {
    return String.format(
        "PersistedValue{name=%s, type=%s, expiration=%s}", name, type.getName(), expiration);
  }

  @Override
  public void markDirty() {
    synced = false;
  }

  @Override
  public boolean isDirty() {
    return !synced;
  }

  @Override
  public void sync() {
    if(synced) return;
    this.accessor.set(this.cachedAccessor.get());
    synced = true;
  }

  @Override
  public void setInactive() {
    throw new NotImplementedException();
  }

  @Override
  public boolean ifActive() {
    throw new NotImplementedException();
  }

  @Override
  public void flush() {
    sync();
  }

  @Override
  public StateDescriptor getDescriptor() {
    return descriptor;
  }

  private static final class NonFaultTolerantAccessor<E> implements Accessor<E> {
    private E element;

    @Override
    public void set(E element) {
      this.element = element;
    }

    @Override
    public E get() {
      return element;
    }

    @Override
    public void clear() {
      element = null;
    }
  }
}
