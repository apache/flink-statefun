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

package com.ververica.statefun.flink.core.functions;

import com.ververica.statefun.flink.common.SetContextClassLoader;
import com.ververica.statefun.flink.core.di.Inject;
import com.ververica.statefun.flink.core.di.Label;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.metrics.FunctionTypeMetrics;
import com.ververica.statefun.flink.core.metrics.MetricsFactory;
import com.ververica.statefun.flink.core.state.BoundState;
import com.ververica.statefun.flink.core.state.StateBinder;
import com.ververica.statefun.sdk.FunctionType;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;

final class StatefulFunctionRepository implements FunctionRepository {
  private final ObjectOpenHashMap<FunctionType, StatefulFunction> instances;
  private final StateBinder stateBinder;
  private final FunctionLoader functionLoader;
  private final MetricsFactory metricsFactory;
  private final MessageFactory messageFactory;

  @Inject
  StatefulFunctionRepository(
      @Label("function-loader") FunctionLoader functionLoader,
      @Label("metrics-factory") MetricsFactory metricsFactory,
      MessageFactory messageFactory,
      StateBinder stateBinder) {
    this.instances = new ObjectOpenHashMap<>();
    this.stateBinder = Objects.requireNonNull(stateBinder);
    this.functionLoader = Objects.requireNonNull(functionLoader);
    this.metricsFactory = Objects.requireNonNull(metricsFactory);
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
    com.ververica.statefun.sdk.StatefulFunction statefulFunction =
        functionLoader.load(functionType);
    try (SetContextClassLoader ignored = new SetContextClassLoader(statefulFunction)) {
      BoundState state = stateBinder.bind(functionType, statefulFunction);
      FunctionTypeMetrics metrics = metricsFactory.forType(functionType);
      return new StatefulFunction(statefulFunction, state, metrics, messageFactory);
    }
  }
}
