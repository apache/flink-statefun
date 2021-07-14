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

import java.util.HashMap;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ValueSpec;

class TestAddressScopedStorage implements AddressScopedStorage {

  private HashMap<ValueSpec, Object> storage = new HashMap<>();

  @Override
  public <T> Optional<T> get(ValueSpec<T> spec) {

    return (Optional<T>) Optional.ofNullable(storage.get(spec));
  }

  @Override
  public <T> void set(ValueSpec<T> spec, T value) {
    storage.put(spec, value);
  }

  @Override
  public <T> void remove(ValueSpec<T> spec) {
    storage.remove(spec);
  }
}
