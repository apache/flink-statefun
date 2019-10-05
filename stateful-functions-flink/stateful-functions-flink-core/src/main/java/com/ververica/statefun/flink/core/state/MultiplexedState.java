/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.state;

import com.ververica.statefun.flink.core.common.KeyBy;
import com.ververica.statefun.flink.core.di.Inject;
import com.ververica.statefun.flink.core.di.Label;
import com.ververica.statefun.flink.core.types.DynamicallyRegisteredTypes;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.state.Accessor;
import com.ververica.statefun.sdk.state.PersistedValue;
import java.util.Objects;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

public final class MultiplexedState implements State {

  private final KeyedStateBackend<Object> keyedStateBackend;
  private final DynamicallyRegisteredTypes types;
  private final MapState<byte[], byte[]> sharedMapStateHandle;
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
    final byte[] uniqueSubKey = multiplexedSubstateKey(functionType, persistedValue.name());
    final TypeSerializer<T> valueSerializer = multiplexedSubstateValueSerializer(persistedValue);
    return new MultiplexedMapStateAccessor<>(sharedMapStateHandle, uniqueSubKey, valueSerializer);
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

  private static MapState<byte[], byte[]> createSharedMapState(RuntimeContext runtimeContext) {
    MapStateDescriptor<byte[], byte[]> descriptor =
        new MapStateDescriptor<>(
            "state",
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

    return runtimeContext.getMapState(descriptor);
  }

  private static byte[] multiplexedSubstateKey(FunctionType functionType, String name) {
    String stateKey =
        String.format("%s.%s.%s", functionType.namespace(), functionType.name(), name);
    return stateKey.getBytes(Charsets.UTF_8);
  }
}
