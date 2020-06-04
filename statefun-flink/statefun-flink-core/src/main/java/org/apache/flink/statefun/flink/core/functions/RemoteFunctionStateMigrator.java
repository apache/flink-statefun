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
package org.apache.flink.statefun.flink.core.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.state.FlinkState;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

/**
 * Performs state migration for legacy remote function state in StateFun versions <= 2.1.x.
 *
 * <p>TODO we plan to remove this backwards compatibility path in version 2.3.0, meaning that TODO
 * users who want to upgrade from 2.1.x to 2.3.x need to first upgrade to 2.2.x.
 */
final class RemoteFunctionStateMigrator
    implements KeyedStateFunction<String, MapState<String, byte[]>> {

  private static final String LEGACY_MUX_STATE_NAME = "states";

  static void apply(
      Map<FunctionType, StatefulFunctionProvider> functionProviders,
      KeyedStateBackend<String> keyedStateBackend,
      TypeInformation<String> keyTypeInfo,
      TypeInformation<byte[]> valueTypeInfo)
      throws Exception {
    functionProviders.entrySet().stream()
        .filter(RemoteFunctionStateMigrator::isRemoteFunctionProvider)
        .forEach(
            remoteFunctionProvider ->
                migrateRemoteFunctionState(
                    remoteFunctionProvider, keyedStateBackend, keyTypeInfo, valueTypeInfo));
  }

  private static boolean isRemoteFunctionProvider(
      Map.Entry<FunctionType, StatefulFunctionProvider> functionProviderEntry) {
    return functionProviderEntry.getValue() instanceof HttpFunctionProvider;
  }

  private static void migrateRemoteFunctionState(
      Map.Entry<FunctionType, StatefulFunctionProvider> functionProviderEntry,
      KeyedStateBackend<String> keyedStateBackend,
      TypeInformation<String> keyTypeInfo,
      TypeInformation<byte[]> valueTypeInfo) {
    final FunctionType functionType = functionProviderEntry.getKey();
    final HttpFunctionSpec functionSpec =
        ((HttpFunctionProvider) functionProviderEntry.getValue()).getFunctionSpec(functionType);

    try {
      final RemoteFunctionStateMigrator stateMigrator =
          new RemoteFunctionStateMigrator(
              demuxValueStateHandles(
                  functionSpec.states(), functionType, keyedStateBackend, valueTypeInfo));

      keyedStateBackend.applyToAllKeys(
          VoidNamespace.INSTANCE,
          VoidNamespaceSerializer.INSTANCE,
          multiplexedStateDescriptor(functionType, keyTypeInfo, valueTypeInfo),
          stateMigrator);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error migrating multiplexed state for remote function type " + functionType);
    }
  }

  /** The value states to de-mux the multiplexed state into. */
  private final Map<String, ValueState<byte[]>> demuxValueStates;

  private RemoteFunctionStateMigrator(Map<String, ValueState<byte[]>> demuxValueStates) {
    this.demuxValueStates = Objects.requireNonNull(demuxValueStates);
  }

  @Override
  public void process(String key, MapState<String, byte[]> multiplexedState) throws Exception {
    for (Map.Entry<String, byte[]> entry : multiplexedState.entries()) {
      final String stateName = entry.getKey();
      final byte[] value = entry.getValue();

      final ValueState<byte[]> demuxState = demuxValueStates.get(stateName);
      // drop state if it is no longer registered, otherwise migrate to value state
      if (demuxState != null) {
        demuxState.update(value);
      }
    }
    multiplexedState.clear();
  }

  private static Map<String, ValueState<byte[]>> demuxValueStateHandles(
      List<StateSpec> stateSpecs,
      FunctionType functionType,
      KeyedStateBackend<String> keyedStateBackend,
      TypeInformation<byte[]> valueTypeInfo)
      throws Exception {
    final Map<String, ValueState<byte[]>> valueStates = new HashMap<>(stateSpecs.size());
    for (StateSpec stateSpec : stateSpecs) {
      valueStates.put(
          stateSpec.name(),
          keyedStateBackend.getOrCreateKeyedState(
              VoidNamespaceSerializer.INSTANCE,
              demuxValueStateDescriptor(functionType, stateSpec, valueTypeInfo)));
    }
    return valueStates;
  }

  private static ValueStateDescriptor<byte[]> demuxValueStateDescriptor(
      FunctionType functionType, StateSpec stateSpec, TypeInformation<byte[]> valueTypeInfo) {
    return new ValueStateDescriptor<>(
        FlinkState.flinkStateName(functionType, stateSpec.name()), valueTypeInfo);
  }

  private static MapStateDescriptor<String, byte[]> multiplexedStateDescriptor(
      FunctionType functionType,
      TypeInformation<String> keyTypeInfo,
      TypeInformation<byte[]> valueTypeInfo) {
    return new MapStateDescriptor<>(
        FlinkState.flinkStateName(functionType, LEGACY_MUX_STATE_NAME), keyTypeInfo, valueTypeInfo);
  }
}
