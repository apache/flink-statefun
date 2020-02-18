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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.state.BoundState.Builder;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.ApiExtension;
import org.apache.flink.statefun.sdk.state.AppendingBufferAccessor;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.TableAccessor;

public final class StateBinder {
  private final State state;

  @Inject
  public StateBinder(@Label("state") State state) {
    this.state = Objects.requireNonNull(state);
  }

  public BoundState bind(FunctionType functionType, @Nullable Object instance) {
    List<?> states = PersistedStates.findReflectively(instance);
    Builder builder = BoundState.builder();
    for (Object persisted : states) {
      if (persisted instanceof PersistedValue) {
        PersistedValue<?> persistedValue = (PersistedValue<?>) persisted;
        Accessor<?> accessor = state.createFlinkStateAccessor(functionType, persistedValue);
        setAccessorRaw(persistedValue, accessor);
        builder.withPersistedValue(persistedValue);
      } else if (persisted instanceof PersistedTable) {
        PersistedTable<?, ?> persistedTable = (PersistedTable<?, ?>) persisted;
        TableAccessor<?, ?> accessor =
            state.createFlinkStateTableAccessor(functionType, persistedTable);
        setAccessorRaw(persistedTable, accessor);
        builder.withPersistedTable(persistedTable);
      } else if (persisted instanceof PersistedAppendingBuffer) {
        PersistedAppendingBuffer<?> persistedAppendingBuffer =
            (PersistedAppendingBuffer<?>) persisted;
        AppendingBufferAccessor<?> accessor =
            state.createFlinkStateAppendingBufferAccessor(functionType, persistedAppendingBuffer);
        setAccessorRaw(persistedAppendingBuffer, accessor);
        builder.withPersistedList(persistedAppendingBuffer);
      } else {
        throw new IllegalArgumentException("Unknown persisted field " + persisted);
      }
    }
    return builder.build();
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
  private static void setAccessorRaw(
      PersistedAppendingBuffer<?> persistedAppendingBuffer, AppendingBufferAccessor<?> accessor) {
    ApiExtension.setPersistedAppendingBufferAccessor(
        (PersistedAppendingBuffer) persistedAppendingBuffer, accessor);
  }
}
