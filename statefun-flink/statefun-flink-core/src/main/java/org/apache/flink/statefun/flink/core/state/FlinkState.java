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

import java.util.ArrayList;
import java.util.List;
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
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableAppendingBuffer;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableList;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableState;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableTable;
import org.apache.flink.statefun.sdk.utils.StateUtils;

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
    if (persistedValue instanceof PartitionedMergeableState){
      PartitionedMergeableState partitionedState = (PartitionedMergeableState)persistedValue;
      List<String> remotePartitionAccessorNames = StateUtils.getPartitionedStateNames(stateName, partitionedState.getNumPartitions());
      ArrayList<Accessor<T>> remotePartitionedAccessors = new ArrayList<>();
      for(String remoteAccessorName : remotePartitionAccessorNames){
        ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(remoteAccessorName, typeInfo);
        configureStateTtl(descriptor, persistedValue.expiration());
        ValueState<T> handle = runtimeContext.getState(descriptor);
        remotePartitionedAccessors.add(new FlinkValueAccessor<>(handle, descriptor));
      }
      partitionedState.setAllRemotePartitionedAccessors(remotePartitionedAccessors);
      ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, typeInfo);
      configureStateTtl(descriptor, persistedValue.expiration());
      ValueState<T> handle = runtimeContext.getState(descriptor);
      persistedValue.setDescriptor(descriptor);
      return new FlinkValueAccessor<>(handle, descriptor);
    }
    else{
      if(typeInfo.getTypeClass()==Long.class){
        IntegerValueStateDescriptor descriptor = new IntegerValueStateDescriptor(stateName, (TypeInformation<Long>) typeInfo, 0L);
        configureStateTtl(descriptor, persistedValue.expiration());
        IntegerValueState handle = (IntegerValueState) runtimeContext.getState(descriptor);
        persistedValue.setDescriptor(descriptor);
        return (Accessor<T>) new FlinkIntegerValueAccessor(handle, descriptor);
      }
      else{
        ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, typeInfo);
        configureStateTtl(descriptor, persistedValue.expiration());
        ValueState<T> handle = runtimeContext.getState(descriptor);
        persistedValue.setDescriptor(descriptor);
        return new FlinkValueAccessor<>(handle, descriptor);
      }
    }

  }

  @Override
  public <T> Accessor<T> createFlinkStateAccessor(FunctionType functionType, PersistedCacheableValue<T> persistedValue) {
    TypeInformation<T> typeInfo = types.registerType(persistedValue.type());
    String stateName = flinkStateName(functionType, persistedValue.name());
    ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, typeInfo);
    configureStateTtl(descriptor, persistedValue.expiration());
    ValueState<T> handle = runtimeContext.getState(descriptor);
    persistedValue.setDescriptor(descriptor);
    return new FlinkValueAccessor<>(handle, descriptor);
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
      persistedValue.setDescriptor(descriptor);
      return (AsyncAccessor<T>) new FlinkAsyncIntegerValueAccessor(handle);
    }
    else{
      AsyncValueStateDescriptor<T> descriptor = new AsyncValueStateDescriptor<>(stateName, typeInfo);
      configureStateTtl(descriptor, persistedValue.expiration());
      AsyncValueState<T> handle = runtimeContext.getAsyncState(descriptor);
      persistedValue.setDescriptor(descriptor);
      return new FlinkAsyncValueAccessor<>(handle);
    }
  }

  @Override
  public <K, V> TableAccessor<K, V> createFlinkStateTableAccessor(
      FunctionType functionType, PersistedTable<K, V> persistedTable) {
    String stateName = flinkStateName(functionType, persistedTable.name());
    if (persistedTable instanceof PartitionedMergeableTable){
      PartitionedMergeableState partitionedState = (PartitionedMergeableTable)persistedTable;
      List<String> remotePartitionAccessorNames = StateUtils.getPartitionedStateNames(stateName, partitionedState.getNumPartitions());
      ArrayList<TableAccessor<K, V>> remotePartitionedAccessors = new ArrayList<>();
      for(String remoteAccessorName : remotePartitionAccessorNames){
        MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(remoteAccessorName,
                types.registerType(persistedTable.keyType()),
                types.registerType(persistedTable.valueType()));
        configureStateTtl(descriptor, persistedTable.expiration());
        MapState<K, V> handle = runtimeContext.getMapState(descriptor);
        remotePartitionedAccessors.add(new FlinkTableAccessor<>(handle));
      }
      partitionedState.setAllRemotePartitionedAccessors(remotePartitionedAccessors);
      MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(stateName,
              types.registerType(persistedTable.keyType()),
              types.registerType(persistedTable.valueType()));
      configureStateTtl(descriptor, persistedTable.expiration());
      MapState<K, V> handle = runtimeContext.getMapState(descriptor);
      persistedTable.setDescriptor(descriptor);
      return new FlinkTableAccessor<>(handle);
    }
    else{
      MapStateDescriptor<K, V> descriptor =
              new MapStateDescriptor<>(
                      flinkStateName(functionType, persistedTable.name()),
                      types.registerType(persistedTable.keyType()),
                      types.registerType(persistedTable.valueType()));
      configureStateTtl(descriptor, persistedTable.expiration());
      MapState<K, V> handle = runtimeContext.getMapState(descriptor);
      persistedTable.setDescriptor(descriptor);
      return new FlinkTableAccessor<>(handle);
    }
  }

  @Override
  public <E> AppendingBufferAccessor<E> createFlinkStateAppendingBufferAccessor(
      FunctionType functionType, PersistedAppendingBuffer<E> persistedAppendingBuffer) {
    String stateName = flinkStateName(functionType, persistedAppendingBuffer.name());
    if (persistedAppendingBuffer instanceof PartitionedMergeableAppendingBuffer){
      PartitionedMergeableState partitionedState = (PartitionedMergeableAppendingBuffer)persistedAppendingBuffer;
      List<String> remotePartitionAccessorNames = StateUtils.getPartitionedStateNames(stateName, partitionedState.getNumPartitions());
      ArrayList<FlinkAppendingBufferAccessor<E>> remotePartitionedAccessors = new ArrayList<>();
      for(String remoteAccessorName : remotePartitionAccessorNames){
        ListStateDescriptor<E> descriptor = new ListStateDescriptor<>(remoteAccessorName,
                types.registerType(persistedAppendingBuffer.elementType()));
        configureStateTtl(descriptor, persistedAppendingBuffer.expiration());
        ListState<E> handle = runtimeContext.getListState(descriptor);
        remotePartitionedAccessors.add(new FlinkAppendingBufferAccessor<>(handle));
      }
      partitionedState.setAllRemotePartitionedAccessors(remotePartitionedAccessors);
      ListStateDescriptor<E> descriptor = new ListStateDescriptor<>(stateName,
              types.registerType(persistedAppendingBuffer.elementType()));
      configureStateTtl(descriptor, persistedAppendingBuffer.expiration());
      ListState<E> handle = runtimeContext.getListState(descriptor);
      persistedAppendingBuffer.setDescriptor(descriptor);
      return new FlinkAppendingBufferAccessor<>(handle);
    }
    else{
      ListStateDescriptor<E> descriptor =
              new ListStateDescriptor<>(
                      flinkStateName(functionType, persistedAppendingBuffer.name()),
                      types.registerType(persistedAppendingBuffer.elementType()));
      configureStateTtl(descriptor, persistedAppendingBuffer.expiration());
      ListState<E> handle = runtimeContext.getListState(descriptor);
      persistedAppendingBuffer.setDescriptor(descriptor);
      return new FlinkAppendingBufferAccessor<>(handle);
    }
  }

  @Override
  public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedList<E> persistedList) {
    String stateName = flinkStateName(functionType, persistedList.name());
    if(persistedList instanceof PartitionedMergeableList){
      PartitionedMergeableState partitionedState = (PartitionedMergeableList)persistedList;
      List<String> remotePartitionAccessorNames = StateUtils.getPartitionedStateNames(stateName, partitionedState.getNumPartitions());
      ArrayList<FlinkListAccessor<E>> remotePartitionedAccessors = new ArrayList<>();
      for(String remoteAccessorName : remotePartitionAccessorNames){
        ListStateDescriptor<E> descriptor = new ListStateDescriptor<>(remoteAccessorName,
                types.registerType(persistedList.elementType()));
        configureStateTtl(descriptor, persistedList.expiration());
        ListState<E> handle = runtimeContext.getListState(descriptor);
        remotePartitionedAccessors.add(new FlinkListAccessor<>(handle));
      }
      partitionedState.setAllRemotePartitionedAccessors(remotePartitionedAccessors);
      ListStateDescriptor<E> descriptor = new ListStateDescriptor<>(stateName,
              types.registerType(persistedList.elementType()));
      configureStateTtl(descriptor, persistedList.expiration());
      ListState<E> handle = runtimeContext.getListState(descriptor);
      persistedList.setDescriptor(descriptor);
      return new FlinkListAccessor<>(handle);
    }
    else{
      ListStateDescriptor<E> descriptor =
              new ListStateDescriptor<>(
                      flinkStateName(functionType, persistedList.name()),
                      types.registerType(persistedList.elementType()));
      configureStateTtl(descriptor, persistedList.expiration());
      ListState<E> handle = runtimeContext.getListState(descriptor);
      persistedList.setDescriptor(descriptor);
      return new FlinkListAccessor<>(handle);
    }
  }

  @Override
  public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedCacheableList<E> persistedList) {
    ListStateDescriptor<E> descriptor =
            new ListStateDescriptor<>(
                    flinkStateName(functionType, persistedList.name()),
                    types.registerType(persistedList.elementType()));
    configureStateTtl(descriptor, persistedList.expiration());
    ListState<E> handle = runtimeContext.getListState(descriptor);
    persistedList.setDescriptor(descriptor);
    return new FlinkListAccessor<>(handle);
  }

  @Override
  public void setCurrentKey(Address address) {
    keyedStateBackend.setCurrentKey(KeyBy.apply(address));
  }

  @Override
  public String getCurrentKey() {
    return (String) keyedStateBackend.getCurrentKey();
  }

  public static String flinkStateName(FunctionType functionType, String name) {
    return String.format("%s.%s.%s", functionType.namespace(), functionType.name(), name);
  }
}
