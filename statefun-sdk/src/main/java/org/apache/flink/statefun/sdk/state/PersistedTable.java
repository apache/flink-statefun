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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;

/**
 * A {@link PersistedTable} is a table (collection of keys and values) registered within {@link
 * StatefulFunction}s and is persisted and maintained by the system for fault-tolerance.
 * This implementation maintains the insertion order.
 *
 * <p>Created persisted tables must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <K> type of the key - Please note that the key have a meaningful {@link #hashCode()} and
 *     {@link #equals(Object)} implemented.
 * @param <V> type of the value.
 */
public final class PersistedTable<K, V> {
  private final String name;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private TableAccessor<K, V> accessor;

  private PersistedTable(
      String name, Class<K> keyType, Class<V> valueType, TableAccessor<K, V> accessor) {
    this.name = Objects.requireNonNull(name);
    this.keyType = Objects.requireNonNull(keyType);
    this.valueType = Objects.requireNonNull(valueType);
    this.accessor = Objects.requireNonNull(accessor);
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
    return new PersistedTable<>(name, keyType, valueType, new NonFaultTolerantAccessor<>());
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

  /**
   * Returns a persisted table's value.
   *
   * @return the persisted table value associated with {@code key}.
   */
  public V get(K key) {
    return accessor.get(key);
  }

  /**
   * Updates the persisted table.
   *
   * @param key the to associate the value with.
   * @param value the new value.
   */
  public void set(K key, V value) {
    accessor.set(key, value);
  }

  /**
   * Removes the value associated with {@code key}.
   *
   * @param key the key to remove.
   */
  public void remove(K key) {
    accessor.remove(key);
  }

  /**
   * Gets an {@link Iterable} over all the entries of the persisted table.
   *
   * @return An {@link Iterable} of the elements of the persisted table.
   */
  public Iterable<Map.Entry<K, V>> entries() {
    return accessor.entries();
  }

  /**
   * Gets an {@link Iterable} over all keys of the persisted table.
   *
   * @return An {@link Iterable} of keys in the persisted table.
   */
  public Iterable<K> keys() {
    return accessor.keys();
  }

  /**
   * Gets an {@link Iterable} over all values of the persisted table.
   *
   * @return An {@link Iterable} of values in the persisted table.
   */
  public Iterable<V> values() {
    return accessor.values();
  }

  /** Clears all elements in the persisted buffer. */
  public void clear() {
    accessor.clear();
  }

  @ForRuntime
  void setAccessor(TableAccessor<K, V> newAccessor) {
    Objects.requireNonNull(newAccessor);
    this.accessor = newAccessor;
  }

  private static final class NonFaultTolerantAccessor<K, V> implements TableAccessor<K, V> {
    private final Map<K, V> map = new LinkedHashMap<>();

    @Override
    public void set(K key, V value) {
      map.put(key, value);
    }

    @Override
    public V get(K key) {
      return map.get(key);
    }

    @Override
    public void remove(K key) {
      map.remove(key);
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
      return map.entrySet();
    }

    @Override
    public Iterable<K> keys() {
      return map.keySet();
    }

    @Override
    public Iterable<V> values() {
      return map.values();
    }

    @Override
    public void clear() {
      map.clear();
    }
  }
}
