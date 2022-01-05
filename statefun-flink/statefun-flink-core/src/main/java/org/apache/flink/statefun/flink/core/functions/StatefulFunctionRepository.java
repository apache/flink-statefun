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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

import akka.actor.Status;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.metrics.FuncionTypeMetricsFactory;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetricsRepository;
import org.apache.flink.statefun.flink.core.state.FlinkStateBinder;
import org.apache.flink.statefun.flink.core.state.PersistedStates;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StatefulFunctionRepository
    implements FunctionRepository, FunctionTypeMetricsRepository {
  private final HashMap<FunctionType, StatefulFunction> instances;
  private final State flinkState;
  private final FunctionLoader functionLoader;
  private final FuncionTypeMetricsFactory metricsFactory;
  private final MessageFactory messageFactory;
  private final Lazy<LocalFunctionGroup> ownerFunctionGroup;
  private final HashMap<String, MailboxState> lastAccessedStatus;
  private static final Logger LOG = LoggerFactory.getLogger(StatefulFunctionRepository.class);

  @Inject
  StatefulFunctionRepository(
      @Label("function-loader") FunctionLoader functionLoader,
      @Label("function-metrics-factory") FuncionTypeMetricsFactory functionMetricsFactory,
      @Label("state") State state,
      @Label("function-group") Lazy<LocalFunctionGroup> localFunctionGroup,
      MessageFactory messageFactory) {
    this.instances = new HashMap<>();
    this.functionLoader = Objects.requireNonNull(functionLoader);
    this.metricsFactory = Objects.requireNonNull(functionMetricsFactory);
    this.flinkState = Objects.requireNonNull(state);
    this.messageFactory = Objects.requireNonNull(messageFactory);
    this.ownerFunctionGroup = localFunctionGroup;
    this.lastAccessedStatus = new HashMap<>();
  }

  @Override
  public LiveFunction get(FunctionType type) {
    StatefulFunction function = instances.get(type);
    if (function == null) {
      instances.put(type, function = load(type));
    }
    if(instances.get(type)==null){
        LOG.error("StatefulFunctionRepository cannot find type " + type + " existing instance " + Arrays.toString(instances.entrySet().stream().map(kv -> kv.getKey() + "->" + (kv.getValue() == null ? "null" : kv.getValue())).toArray()));
    }
    return function;
  }

  @Override
  public void updateStatus(Address address, MailboxState status) {
    System.out.println("Update status address " + address + " to status " + status);
    lastAccessedStatus.put(address.toString(), status);
  }

  @Override
  public MailboxState getStatus(Address address) {
    MailboxState status = lastAccessedStatus.get(address.toString());
    System.out.println("Get status address " + address + " to status " + (status == null?"null":status));
    return status;
  }

  @Override
  public FunctionTypeMetrics getMetrics(FunctionType functionType) {
    return get(functionType).metrics();
  }

  private StatefulFunction load(FunctionType functionType) {
    org.apache.flink.statefun.sdk.StatefulFunction statefulFunction =
        functionLoader.load(functionType);
    System.out.println("Load FunctionType " + functionType + " wid " + Thread.currentThread().getName());
    try (SetContextClassLoader ignored = new SetContextClassLoader(statefulFunction)) {
      FlinkStateBinder stateBinderForType = new FlinkStateBinder(flinkState, functionType);
      PersistedStates.findReflectivelyAndBind(statefulFunction, stateBinderForType);
      FunctionTypeMetrics metrics = metricsFactory.forType(functionType);
      return new StatefulFunction(statefulFunction, metrics, messageFactory, ownerFunctionGroup);
    }
  }
}
