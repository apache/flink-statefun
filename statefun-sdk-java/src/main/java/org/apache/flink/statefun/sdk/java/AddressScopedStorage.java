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
package org.apache.flink.statefun.sdk.java;

import java.util.Optional;
import org.apache.flink.statefun.sdk.java.storage.IllegalStorageAccessException;

/**
 * An {@link AddressScopedStorage} is used for reading and writing persistent values that is managed
 * by Stateful Functions for fault-tolerance and consistency.
 *
 * <p>All access to the storage is scoped to the current invoked function instance, identified by
 * the instance's {@link Address}. This means that within an invocation, function instances may only
 * access it's own persisted values through this storage.
 */
public interface AddressScopedStorage {

  /**
   * Gets the value of the provided {@link ValueSpec}, scoped to the current invoked {@link
   * Address}.
   *
   * @param spec the {@link ValueSpec} to read the value for.
   * @param <T> the type of the value.
   * @return the value, or {@link Optional#empty()} if there was not prior value set.
   * @throws IllegalStorageAccessException if the provided {@link ValueSpec} is not recognized by
   *     the storage (e.g., if it wasn't registered for the accessing function).
   */
  <T> Optional<T> get(ValueSpec<T> spec);

  /**
   * Sets the value for the provided {@link ValueSpec}, scoped to the current invoked {@link
   * Address}.
   *
   * @param spec the {@link ValueSpec} to write the new value for.
   * @param value the new value to set.
   * @param <T> the type of the value.
   * @throws IllegalStorageAccessException if the provided {@link ValueSpec} is not recognized by
   *     the storage (e.g., if it wasn't registered for the accessing function).
   */
  <T> void set(ValueSpec<T> spec, T value);

  /**
   * Removes the prior value set for the provided {@link ValueSpec}, scoped to the current invoked
   * {@link Address}.
   *
   * <p>After removing the value, calling {@link #get(ValueSpec)} for the same spec will return an
   * {@link Optional#empty()}.
   *
   * @param spec the {@link ValueSpec} to remove the prior value for.
   * @param <T> the type of the value.
   * @throws IllegalStorageAccessException if the provided {@link ValueSpec} is not recognized by
   *     the storage (e.g., if it wasn't registered for the accessing function).
   */
  <T> void remove(ValueSpec<T> spec);
}
