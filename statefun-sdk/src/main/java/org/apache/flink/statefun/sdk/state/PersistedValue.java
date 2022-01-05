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

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.flink.api.common.state.StateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;

/**
 * A {@link PersistedValue} is a value registered within {@link StatefulFunction}s and is persisted
 * and maintained by the system for fault-tolerance.
 *
 * <p>Created persisted values must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <T> type of the state.
 */
public class PersistedValue<T> extends ManagedState{
  private static final Logger LOG = LoggerFactory.getLogger(PersistedValue.class);
  private final String name;
  private final Class<T> type;
  private final Expiration expiration;
  protected Accessor<T> cachingAccessor;
  protected Accessor<T> accessor;
  private final Boolean nonFaultTolerant;
  private StateDescriptor descriptor;

  protected PersistedValue(String name, Class<T> type, Expiration expiration, Accessor<T> accessor, Boolean nftFlag) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.expiration = Objects.requireNonNull(expiration);
    if(!(accessor instanceof NonFaultTolerantAccessor)){
      LOG.error("cachingAccessor not of type NonFaultTolerantAccessor.");
    }
    this.cachingAccessor = (NonFaultTolerantAccessor<T>)Objects.requireNonNull(accessor);
    this.accessor = Objects.requireNonNull(accessor);
    this.nonFaultTolerant = Objects.requireNonNull(nftFlag);
    this.descriptor = null;
  }

  /**
   * Creates a {@link PersistedValue} instance that may be used to access persisted state managed by
   * the system. Access to the persisted value is identified by an unique name and type of the
   * value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param type the type of the state values of this {@code PersistedValue}.
   * @param <T> the type of the state values.
   * @return a {@code PersistedValue} instance.
   */
  public static <T> PersistedValue<T> of(String name, Class<T> type) {
    return of(name, type, Expiration.none());
  }


  public static <T> PersistedValue<T> of(String name, Class<T> type, Boolean nonFaultTolerant) {
    return of(name, type, Expiration.none(), nonFaultTolerant);
  }

  /**
   * Creates a {@link PersistedValue} instance that may be used to access persisted state managed by
   * the system. Access to the persisted value is identified by an unique name and type of the
   * value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param type the type of the state values of this {@code PersistedValue}.
   * @param expiration state expiration configuration.
   * @param <T> the type of the state values.
   * @return a {@code PersistedValue} instance.
   */
  public static <T> PersistedValue<T> of(String name, Class<T> type, Expiration expiration) {
    return new PersistedValue<>(name, type, expiration, new NonFaultTolerantAccessor<>(), false);
  }

  public static <T> PersistedValue<T> of(String name, Class<T> type, Expiration expiration, Boolean nftFlag) {
    return new PersistedValue<>(name, type, expiration, new NonFaultTolerantAccessor<>(), nftFlag);
  }

  /**
   * Returns the unique name of the persisted value.
   *
   * @return unique name of the persisted value.
   */
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
    return cachingAccessor.get();
  }

  /**
   * Updates the persisted value.
   *
   * @param value the new value.
   */
  public void set(T value) {
    cachingAccessor.set(value);
  }

  /** Clears the persisted value. After being cleared, the value would be {@code null}. */
  public void clear() {
    cachingAccessor.clear();
  }

  /**
   * Updates the persisted value and returns it, in a single operation.
   *
   * @param update function to process the previous value to obtain the new value.
   * @return the new updated value.
   */
  public T updateAndGet(Function<T, T> update) {
    T current = cachingAccessor.get();
    T updated = update.apply(current);
    cachingAccessor.set(updated);
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
    T value = cachingAccessor.get();
    return value != null ? value : orElse;
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
    T value = cachingAccessor.get();
    return value != null ? value : defaultSupplier.get();
  }

  @ForRuntime
  public void setAccessor(Accessor<T> newAccessor) {
    this.accessor = Objects.requireNonNull(newAccessor);
    ((NonFaultTolerantAccessor<T>)this.cachingAccessor).initialize(newAccessor);
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
  public Boolean ifNonFaultTolerance() {
      return nonFaultTolerant;
  }

  @Override
  public void setInactive() { ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false); }

  @Override
  public void flush() {
    if(((NonFaultTolerantAccessor<T>)this.cachingAccessor).ifActive()){
      this.accessor.set(this.cachingAccessor.get());
      ((NonFaultTolerantAccessor<T>)this.cachingAccessor).setActive(false);
    }
  }

  @Override
  public StateDescriptor getDescriptor() {
    return descriptor;
  }

  public static final class NonFaultTolerantAccessor<E> implements Accessor<E>, CachedAccessor {
    private E element;
    private Accessor<E> remoteAccessor;
    private boolean active;
    private boolean modified;

    public void initialize(Accessor<E> remote){
      remoteAccessor = remote;
      element = remoteAccessor.get();
      active = true;
      modified = false;
    }

    @Override
    public boolean ifActive() {
      return active;
    }

    @Override
    public boolean ifModified() {
      return modified;
    }

    @Override
    public void setActive(boolean active) {
      this.active = active;
    }

    @Override
    public void verifyValid() {
      if(!active){
        initialize(this.remoteAccessor);
      }
    }

    @Override
    public void set(E element) {
      verifyValid();
      this.element = element;
      this.modified = true;
    }

    @Override
    public E get() {
      verifyValid();
      return element;
    }

    @Override
    public void clear() {
      verifyValid();
      element = null;
      this.modified = true;
    }
  }
}
