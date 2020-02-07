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
package org.apache.flink.statefun.flink.core.state;

import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.statefun.sdk.state.TableAccessor;

final class FlinkTableAccessor<K, V> implements TableAccessor<K, V> {

  private final MapState<K, V> handle;

  FlinkTableAccessor(MapState<K, V> handle) {
    this.handle = Objects.requireNonNull(handle);
  }

  @Override
  public void set(K key, V value) {
    try {
      handle.put(key, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V get(K key) {
    try {
      return handle.get(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(K key) {
    try {
      handle.remove(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
