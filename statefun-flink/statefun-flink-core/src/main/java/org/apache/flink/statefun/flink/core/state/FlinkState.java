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

import static org.apache.flink.statefun.flink.core.state.ExpirationUtil.configureStateTtl;

import java.util.Objects;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.types.DynamicallyRegisteredTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.*;

public final class FlinkState implements State {

  private final RuntimeContext runtimeContext;
  private final KeyedStateBackend<Object> keyedStateBackend;
  private final DynamicallyRegisteredTypes types;

  @Inject
  public FlinkState(
      @Label("runtime-context") RuntimeContext runtimeContext,
      @Label("keyed-state-backend") KeyedStateBackend<Object> keyedStateBackend,
      DynamicallyRegisteredTypes types) {

    this.runtimeContext = Objects.requireNonNull(runtimeContext);
    this.keyedStateBackend = Objects.requireNonNull(keyedStateBackend);
    this.types = Objects.requireNonNull(types);
  }

  @Override
  public <T> Accessor<T> createFlinkStateAccessor(
      FunctionType functionType, PersistedValue<T> persistedValue) {
    TypeInformation<T> typeInfo = types.registerType(persistedValue.type());
    String stateName = flinkStateName(functionType, persistedValue.name());
    if(typeInfo.getTypeClass()==Long.class){
      IntegerValueStateDescriptor descriptor = new IntegerValueStateDescriptor(stateName, (TypeInformation<Long>) typeInfo, 0L);
      configureStateTtl(descriptor, persistedValue.expiration());
      IntegerValueState handle = (IntegerValueState) runtimeContext.getState(descriptor);
      return (Accessor<T>) new FlinkIntegerValueAccessor(handle);
    }
    else{
      ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, typeInfo);
      configureStateTtl(descriptor, persistedValue.expiration());
      ValueState<T> handle = runtimeContext.getState(descriptor);
      return new FlinkValueAccessor<>(handle);
    }
  }

  @Override
  public <T> Accessor<T> createFlinkStateAccessor(FunctionType functionType, PersistedCacheableValue<T> persistedValue) {
    TypeInformation<T> typeInfo = types.registerType(persistedValue.type());
    String stateName = flinkStateName(functionType, persistedValue.name());
    ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, typeInfo);
    configureStateTtl(descriptor, persistedValue.expiration());
    ValueState<T> handle = runtimeContext.getState(descriptor);
    return new FlinkValueAccessor<>(handle);
  }

  @Override
  public <T> AsyncAccessor<T> createFlinkAsyncStateAccessor(
          FunctionType functionType, PersistedAsyncValue<T> persistedValue) {
    TypeInformation<T> typeInfo = types.registerType(persistedValue.type());
    String stateName = flinkStateName(functionType, persistedValue.name());
    if(typeInfo.getTypeClass()==Long.class){
      AsyncIntegerValueStateDescriptor descriptor = new AsyncIntegerValueStateDescriptor(stateName, (TypeInformation<Long>) typeInfo, 0L);
      configureStateTtl(descriptor, persistedValue.expiration());
      AsyncIntegerValueState handle = (AsyncIntegerValueState) runtimeContext.getAsyncState(descriptor);
      return (AsyncAccessor<T>) new FlinkAsyncIntegerValueAccessor(handle);
    }
    else{
      AsyncValueStateDescriptor<T> descriptor = new AsyncValueStateDescriptor<>(stateName, typeInfo);
      configureStateTtl(descriptor, persistedValue.expiration());
      AsyncValueState<T> handle = runtimeContext.getAsyncState(descriptor);
      return new FlinkAsyncValueAccessor<>(handle);
    }
  }

  @Override
  public <K, V> TableAccessor<K, V> createFlinkStateTableAccessor(
      FunctionType functionType, PersistedTable<K, V> persistedTable) {

    MapStateDescriptor<K, V> descriptor =
        new MapStateDescriptor<>(
            flinkStateName(functionType, persistedTable.name()),
            types.registerType(persistedTable.keyType()),
            types.registerType(persistedTable.valueType()));

    configureStateTtl(descriptor, persistedTable.expiration());
    MapState<K, V> handle = runtimeContext.getMapState(descriptor);
    return new FlinkTableAccessor<>(handle);
  }

  @Override
  public <E> AppendingBufferAccessor<E> createFlinkStateAppendingBufferAccessor(
      FunctionType functionType, PersistedAppendingBuffer<E> persistedAppendingBuffer) {
    ListStateDescriptor<E> descriptor =
        new ListStateDescriptor<>(
            flinkStateName(functionType, persistedAppendingBuffer.name()),
            types.registerType(persistedAppendingBuffer.elementType()));
    configureStateTtl(descriptor, persistedAppendingBuffer.expiration());
    ListState<E> handle = runtimeContext.getListState(descriptor);
    return new FlinkAppendingBufferAccessor<>(handle);
  }

  @Override
  public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedList<E> persistedList) {
    ListStateDescriptor<E> descriptor =
            new ListStateDescriptor<>(
                    flinkStateName(functionType, persistedList.name()),
                    types.registerType(persistedList.elementType()));
    configureStateTtl(descriptor, persistedList.expiration());
    ListState<E> handle = runtimeContext.getListState(descriptor);
    return new FlinkListAccessor<>(handle);
  }

  @Override
  public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedCacheableList<E> persistedList) {
    ListStateDescriptor<E> descriptor =
            new ListStateDescriptor<>(
                    flinkStateName(functionType, persistedList.name()),
                    types.registerType(persistedList.elementType()));
    configureStateTtl(descriptor, persistedList.expiration());
    ListState<E> handle = runtimeContext.getListState(descriptor);
    return new FlinkListAccessor<>(handle);
  }

  @Override
  public void setCurrentKey(Address address) {
    keyedStateBackend.setCurrentKey(KeyBy.apply(address));
  }

  public static String flinkStateName(FunctionType functionType, String name) {
    return String.format("%s.%s.%s", functionType.namespace(), functionType.name(), name);
  }
}
