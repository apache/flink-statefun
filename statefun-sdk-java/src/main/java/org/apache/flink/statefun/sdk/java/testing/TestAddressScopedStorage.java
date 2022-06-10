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
package org.apache.flink.statefun.sdk.java.testing;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ValueSpec;

final class TestAddressScopedStorage implements AddressScopedStorage {

  private final ConcurrentHashMap<String, Object> storage = new ConcurrentHashMap<>();

  @Override
  public <T> Optional<T> get(ValueSpec<T> spec) {
    Objects.requireNonNull(spec);
    Object value = storage.get(spec.name());

    @SuppressWarnings("unchecked")
    Optional<T> maybeValue = (Optional<T>) Optional.ofNullable(value);
    return maybeValue;
  }

  @Override
  public <T> void set(ValueSpec<T> spec, T value) {
    Objects.requireNonNull(spec);
    Objects.requireNonNull(value);
    storage.put(spec.name(), value);
  }

  @Override
  public <T> void remove(ValueSpec<T> spec) {
    Objects.requireNonNull(spec);
    storage.remove(spec.name());
  }
}
