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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufTypeInformation;
import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.generated.MultiplexedStateKey;
import org.apache.flink.statefun.flink.core.types.DynamicallyRegisteredTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.TableAccessor;

public final class MultiplexedState implements State {

  private final KeyedStateBackend<Object> keyedStateBackend;
  private final DynamicallyRegisteredTypes types;
  private final MapState<MultiplexedStateKey, byte[]> sharedMapStateHandle;
  private final ExecutionConfig executionConfiguration;

  @Inject
  public MultiplexedState(
      @Label("runtime-context") RuntimeContext runtimeContext,
      @Label("keyed-state-backend") KeyedStateBackend<Object> keyedStateBackend,
      DynamicallyRegisteredTypes types) {

    this.keyedStateBackend = Objects.requireNonNull(keyedStateBackend);
    this.types = Objects.requireNonNull(types);
    this.sharedMapStateHandle = createSharedMapState(runtimeContext);
    this.executionConfiguration = Objects.requireNonNull(runtimeContext.getExecutionConfig());
  }

  @Override
  public <T> Accessor<T> createFlinkStateAccessor(
      FunctionType functionType, PersistedValue<T> persistedValue) {
    final MultiplexedStateKey uniqueSubKey =
        multiplexedSubstateKey(functionType, persistedValue.name());
    final TypeSerializer<T> valueSerializer = multiplexedSubstateValueSerializer(persistedValue);
    return new MultiplexedMapStateAccessor<>(sharedMapStateHandle, uniqueSubKey, valueSerializer);
  }

  @Override
  public <K, V> TableAccessor<K, V> createFlinkStateTableAccessor(
      FunctionType functionType, PersistedTable<K, V> persistedTable) {
    final MultiplexedStateKey uniqueSubKeyPrefix =
        multiplexedSubstateKey(functionType, persistedTable.name());
    final TypeSerializer<K> keySerializer =
        types.registerType(persistedTable.keyType()).createSerializer(executionConfiguration);
    final TypeSerializer<V> valueSerializer =
        types.registerType(persistedTable.valueType()).createSerializer(executionConfiguration);
    return new MultiplexedTableStateAccessor<>(
        sharedMapStateHandle, uniqueSubKeyPrefix, keySerializer, valueSerializer);
  }

  @Override
  public void setCurrentKey(Address address) {
    keyedStateBackend.setCurrentKey(KeyBy.apply(address));
  }

  private <T> TypeSerializer<T> multiplexedSubstateValueSerializer(
      PersistedValue<T> persistedValue) {
    TypeInformation<T> typeInfo = types.registerType(persistedValue.type());
    return typeInfo.createSerializer(executionConfiguration);
  }

  private static MapState<MultiplexedStateKey, byte[]> createSharedMapState(
      RuntimeContext runtimeContext) {
    MapStateDescriptor<MultiplexedStateKey, byte[]> descriptor =
        new MapStateDescriptor<>(
            "state",
            new ProtobufTypeInformation<>(MultiplexedStateKey.class),
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

    return runtimeContext.getMapState(descriptor);
  }

  private static MultiplexedStateKey multiplexedSubstateKey(
      FunctionType functionType, String name) {
    return MultiplexedStateKey.newBuilder()
        .setFunctionNamespace(functionType.namespace())
        .setFunctionType(functionType.name())
        .setStateName(name)
        .build();
  }
}
