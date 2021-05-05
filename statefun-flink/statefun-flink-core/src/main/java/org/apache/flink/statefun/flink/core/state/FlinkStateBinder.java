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
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.*;
import org.apache.flink.statefun.sdk.state.PersistedCacheableValue;

/**
 * A {@link StateBinder} that binds persisted state objects to Flink state for a specific {@link
 * FunctionType}.
 */
public final class FlinkStateBinder extends StateBinder {
  private final State state;
  private final FunctionType functionType;

  @Inject
  public FlinkStateBinder(State state, FunctionType functionType) {
    this.state = Objects.requireNonNull(state);
    this.functionType = Objects.requireNonNull(functionType);
  }

  @Override
  public void bindValue(PersistedValue<?> persistedValue) {
    Accessor<?> accessor = state.createFlinkStateAccessor(functionType, persistedValue);
    setAccessorRaw(persistedValue, accessor);
  }

  @Override
  public void bindCacheableValue(PersistedCacheableValue<?> persistedValue) {
    Accessor<?> accessor = state.createFlinkStateAccessor(functionType, persistedValue);
    setAccessorRaw(persistedValue, accessor);
  }

  @Override
  public void bindAsyncValue(PersistedAsyncValue<?> persistedValue) {
    AsyncAccessor<?> accessor = state.createFlinkAsyncStateAccessor(functionType, persistedValue);
    setAsyncAccessorRaw(persistedValue, accessor);
  }

  @Override
  public void bindTable(PersistedTable<?, ?> persistedTable) {
    TableAccessor<?, ?> accessor =
        state.createFlinkStateTableAccessor(functionType, persistedTable);
    setAccessorRaw(persistedTable, accessor);
  }

  @Override
  public void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer) {
    AppendingBufferAccessor<?> accessor =
        state.createFlinkStateAppendingBufferAccessor(functionType, persistedAppendingBuffer);
    setAccessorRaw(persistedAppendingBuffer, accessor);
  }

  @Override
  public void bindList(PersistedList<?> persistedList) {
    ListAccessor<?> accessor = state.createFlinkListStateAccessor(functionType, persistedList);
    setAccessorRaw(persistedList, accessor);
  }

  @Override
  public void bindCacheableList(PersistedCacheableList<?> persistedList) {
    ListAccessor<?> accessor = state.createFlinkListStateAccessor(functionType, persistedList);
    setAccessorRaw(persistedList, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void setAccessorRaw(PersistedTable<?, ?> persistedTable, TableAccessor<?, ?> accessor) {
    ApiExtension.setPersistedTableAccessor((PersistedTable) persistedTable, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAccessorRaw(PersistedValue<?> persistedValue, Accessor<?> accessor) {
    ApiExtension.setPersistedValueAccessor((PersistedValue) persistedValue, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAccessorRaw(PersistedCacheableValue<?> persistedValue, Accessor<?> accessor) {
    ApiExtension.setPersistedValueAccessor((PersistedCacheableValue) persistedValue, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAsyncAccessorRaw(PersistedAsyncValue<?> persistedValue, AsyncAccessor<?> accessor) {
    ApiExtension.setPersistedAsyncValueAccessor((PersistedAsyncValue) persistedValue, accessor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setAccessorRaw(
      PersistedAppendingBuffer<?> persistedAppendingBuffer, AppendingBufferAccessor<?> accessor) {
    ApiExtension.setPersistedAppendingBufferAccessor(
        (PersistedAppendingBuffer) persistedAppendingBuffer, accessor);
  }

  private static void setAccessorRaw(
          PersistedList<?> persistedList, ListAccessor<?> accessor) {
    ApiExtension.setPersistedListAccessor(
            (PersistedList) persistedList, accessor);
  }

  private static void setAccessorRaw(
          PersistedCacheableList<?> persistedList, ListAccessor<?> accessor) {
    ApiExtension.setPersistedListAccessor(
            (PersistedCacheableList) persistedList, accessor);
  }
}
