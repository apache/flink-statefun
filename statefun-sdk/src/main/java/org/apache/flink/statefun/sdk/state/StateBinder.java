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

public abstract class StateBinder {
  public abstract void bindValue(PersistedValue<?> persistedValue);

  public abstract void bindCacheableValue(PersistedCacheableValue<?> persistedValue);

  public abstract void bindAsyncValue(PersistedAsyncValue<?> persistedValue);

  public abstract void bindTable(PersistedTable<?, ?> persistedTable);

  public abstract void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer);

  public abstract void bindList(PersistedList<?> persistedList);

  public abstract void bindCacheableList(PersistedCacheableList<?> persistedList);

  public final void bind(Object stateObject) {
    if (stateObject instanceof PersistedValue) {
      bindValue((PersistedValue<?>) stateObject);
    } else if (stateObject instanceof PersistedAsyncValue) {
      bindAsyncValue((PersistedAsyncValue<?>) stateObject);
    } else if (stateObject instanceof PersistedTable) {
      bindTable((PersistedTable<?, ?>) stateObject);
    } else if (stateObject instanceof PersistedAppendingBuffer) {
      bindAppendingBuffer((PersistedAppendingBuffer<?>) stateObject);
    } else if (stateObject instanceof PersistedList) {
      bindList((PersistedList<?>) stateObject);
    } else if (stateObject instanceof PersistedCacheableList) {
      bindCacheableList((PersistedCacheableList<?>) stateObject);
    } else if (stateObject instanceof PersistedCacheableValue) {
      bindCacheableValue((PersistedCacheableValue<?>) stateObject);
    } else {
      throw new IllegalArgumentException("Unknown persisted state object " + stateObject);
    }
  }
}
