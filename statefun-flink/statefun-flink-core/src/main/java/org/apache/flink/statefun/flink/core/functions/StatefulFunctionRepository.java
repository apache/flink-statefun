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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.metrics.MetricsFactory;
import org.apache.flink.statefun.flink.core.state.FlinkStateBinder;
import org.apache.flink.statefun.flink.core.state.PersistedStates;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.sdk.FunctionType;

final class StatefulFunctionRepository implements FunctionRepository {
  private final ObjectOpenHashMap<FunctionType, StatefulFunction> instances;
  private final State flinkState;
  private final FunctionLoader functionLoader;
  private final MetricsFactory metricsFactory;
  private final MessageFactory messageFactory;

  @Inject
  StatefulFunctionRepository(
      @Label("function-loader") FunctionLoader functionLoader,
      @Label("metrics-factory") MetricsFactory metricsFactory,
      @Label("state") State state,
      MessageFactory messageFactory) {
    this.instances = new ObjectOpenHashMap<>();
    this.functionLoader = Objects.requireNonNull(functionLoader);
    this.metricsFactory = Objects.requireNonNull(metricsFactory);
    this.flinkState = Objects.requireNonNull(state);
    this.messageFactory = Objects.requireNonNull(messageFactory);
  }

  @Override
  public LiveFunction get(FunctionType type) {
    StatefulFunction function = instances.get(type);
    if (function == null) {
      instances.put(type, function = load(type));
    }
    return function;
  }

  private StatefulFunction load(FunctionType functionType) {
    org.apache.flink.statefun.sdk.StatefulFunction statefulFunction =
        functionLoader.load(functionType);
    try (SetContextClassLoader ignored = new SetContextClassLoader(statefulFunction)) {
      FlinkStateBinder stateBinderForType = new FlinkStateBinder(flinkState, functionType);
      PersistedStates.findAndBind(statefulFunction, stateBinderForType);
      FunctionTypeMetrics metrics = metricsFactory.forType(functionType);
      return new StatefulFunction(statefulFunction, metrics, messageFactory);
    }
  }
}
