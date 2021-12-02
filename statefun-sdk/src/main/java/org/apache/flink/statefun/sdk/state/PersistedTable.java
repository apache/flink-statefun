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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;

/**
 * A {@link PersistedTable} is a table (collection of keys and values) registered within {@link
 * StatefulFunction}s and is persisted and maintained by the system for fault-tolerance.
 *
 * <p>Created persisted tables must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <K> type of the key - Please note that the key have a meaningful {@link #hashCode()} and
 *     {@link #equals(Object)} implemented.
 * @param <V> type of the value.
 */
public class PersistedTable<K, V> extends ManagedState {
  private static final Logger LOG = LoggerFactory.getLogger(PersistedTable.class);
  private final String name;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Expiration expiration;
  protected NonFaultTolerantAccessor<K, V> cachingAccessor;
  protected TableAccessor<K, V> accessor;
  private final Boolean nonFaultTolerant;

  public PersistedTable(
      String name,
      Class<K> keyType,
      Class<V> valueType,
      Expiration expiration,
      TableAccessor<K, V> accessor,
      Boolean nftFlag) {
    this.name = Objects.requireNonNull(name);
    this.keyType = Objects.requireNonNull(keyType);
    this.valueType = Objects.requireNonNull(valueType);
    this.expiration = Objects.requireNonNull(expiration);
    if(!(cachingAccessor instanceof NonFaultTolerantAccessor)){
      LOG.error("cachingAccessor not of type NonFaultTolerantAccessor.");
    }
    this.cachingAccessor = (NonFaultTolerantAccessor<K, V>)Objects.requireNonNull(accessor);
    this.accessor = Objects.requireNonNull(accessor);
    this.nonFaultTolerant = Objects.requireNonNull(nftFlag);
  }

  /**
   * Creates a {@link PersistedTable} instance that may be used to access persisted state managed by
   * the system. Access to the persisted table is identified by an unique name, type of the key, and
   * type of the value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param keyType the type of the state keys of this {@code PersistedTable}.
   * @param valueType the type of the state values of this {@code PersistedTale}.
   * @param <K> the type of the state keys.
   * @param <V> the type of the state values.
   * @return a {@code PersistedTable} instance.
   */
  public static <K, V> PersistedTable<K, V> of(String name, Class<K> keyType, Class<V> valueType) {
    return of(name, keyType, valueType, Expiration.none(), false);
  }

  public static <K, V> PersistedTable<K, V> of(String name, Class<K> keyType, Class<V> valueType, Boolean nftFlag) {
    return of(name, keyType, valueType, Expiration.none(), nftFlag);
  }

  /**
   * Creates a {@link PersistedTable} instance that may be used to access persisted state managed by
   * the system. Access to the persisted table is identified by an unique name, type of the key, and
   * type of the value. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted state.
   * @param keyType the type of the state keys of this {@code PersistedTable}.
   * @param valueType the type of the state values of this {@code PersistedTale}.
   * @param expiration state expiration configuration.
   * @param <K> the type of the state keys.
   * @param <V> the type of the state values.
   * @return a {@code PersistedTable} instance.
   */
  public static <K, V> PersistedTable<K, V> of(
      String name, Class<K> keyType, Class<V> valueType, Expiration expiration, Boolean nonFaultTolerant) {
    return new PersistedTable<>(
        name, keyType, valueType, expiration, new NonFaultTolerantAccessor<>(), nonFaultTolerant);
  }

  /**
   * Returns the unique name of the persisted table.
   *
   * @return unique name of the persisted table.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the persisted tables keys.
   *
   * @return the type of the persisted tables keys.
   */
  public Class<K> keyType() {
    return keyType;
  }

  /**
   * Returns the type of the persisted tables values.
   *
   * @return the type of the persisted tables values.
   */
  public Class<V> valueType() {
    return valueType;
  }

  public Expiration expiration() {
    return expiration;
  }

  /**
   * Returns a persisted table's value.
   *
   * @return the persisted table value associated with {@code key}.
   */
  public V get(K key) {
    return cachingAccessor.get(key);
  }

  /**
   * Updates the persisted table.
   *
   * @param key the to associate the value with.
   * @param value the new value.
   */
  public void set(K key, V value) {
    cachingAccessor.set(key, value);
  }

  /**
   * Remov
   * s the value associated with {@code key}.
   *
   * @param key the key to remove.
   */
  public void remove(K key) {
    cachingAccessor.remove(key);
  }

  /**
   * Gets an {@link Iterable} over all the entries of the persisted table.
   *
   * @return An {@link Iterable} of the elements of the persisted table.
   */
  public Iterable<Map.Entry<K, V>> entries() {
    return cachingAccessor.entries();
  }

  /**
   * Gets an {@link Iterable} over all keys of the persisted table.
   *
   * @return An {@link Iterable} of keys in the persisted table.
   */
  public Iterable<K> keys() {
    return cachingAccessor.keys();
  }

  /**
   * Gets an {@link Iterable} over all values of the persisted table.
   *
   * @return An {@link Iterable} of values in the persisted table.
   */
  public Iterable<V> values() {
    return cachingAccessor.values();
  }

  /** Clears all elements in the persisted buffer. */
  public void clear() {
    cachingAccessor.clear();
  }

  @Override
  public String toString() {
    return String.format(
        "PersistedTable{name=%s, keyType=%s, valueType=%s, expiration=%s}",
        name, keyType.getName(), valueType.getName(), expiration);
  }

  @ForRuntime
  void setAccessor(TableAccessor<K, V> newAccessor) {
    this.accessor = Objects.requireNonNull(newAccessor);
    this.cachingAccessor.initialize(newAccessor);
  }

  @Override
  public Boolean ifNonFaultTolerance() {
    return nonFaultTolerant;
  }

  @Override
  public void setInactive() {
    this.cachingAccessor.setActive(false);
  }

  @Override
  public void flush() {
    if(this.cachingAccessor.ifActive()){
      this.accessor.clear();
      for(Map.Entry<K, V> pair : this.cachingAccessor.entries()){
        this.accessor.set(pair.getKey(), pair.getValue());
      }
    }
  }

  public static final class NonFaultTolerantAccessor<K, V> implements TableAccessor<K, V>, CachedAccessor {
    private Map<K, V> map = new HashMap<>();
    private TableAccessor<K, V> remoteAccesor;
    private boolean active;
    private boolean modified;

    public void initialize(TableAccessor<K, V> remote){
      remoteAccesor = remote;
      map = (Map<K, V>)remoteAccesor.entries();
      active = true;
      modified = false;
    }

    @Override
    public void set(K key, V value) {
      verifyValid();
      map.put(key, value);
      modified = true;
    }

    @Override
    public V get(K key) {
      verifyValid();
      return map.get(key);
    }

    @Override
    public void remove(K key) {
      verifyValid();
      map.remove(key);
      modified = true;
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
      verifyValid();
      return map.entrySet();
    }

    @Override
    public Iterable<K> keys() {
      verifyValid();
      return map.keySet();
    }

    @Override
    public Iterable<V> values() {
      verifyValid();
      return map.values();
    }

    @Override
    public void clear() {
      verifyValid();
      map.clear();
      modified = true;
    }

    @Override
    public boolean ifActive() {
      return active;
    }

    @Override
    public boolean ifModified() {
      return false;
    }

    @Override
    public void setActive(boolean active) {
      this.active = active;
    }

    @Override
    public void verifyValid() {
      if(!active){
        initialize(this.remoteAccesor);
      }
    }
  }
}
