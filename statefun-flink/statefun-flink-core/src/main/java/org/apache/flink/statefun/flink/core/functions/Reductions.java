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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Lazy;
import org.apache.flink.statefun.flink.core.di.ObjectContainer;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.metrics.FlinkMetricsFactory;
import org.apache.flink.statefun.flink.core.metrics.MetricsFactory;
import org.apache.flink.statefun.flink.core.state.FlinkState;
import org.apache.flink.statefun.flink.core.state.MultiplexedState;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.flink.core.state.StateBinder;
import org.apache.flink.statefun.flink.core.types.DynamicallyRegisteredTypes;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

final class Reductions {
  private final LocalFunctionGroup localFunctionGroup;

  @Inject
  Reductions(LocalFunctionGroup functionGroup) {
    this.localFunctionGroup = Objects.requireNonNull(functionGroup);
  }

  static Reductions create(
      Configuration configuration,
      StatefulFunctionsUniverse statefulFunctionsUniverse,
      RuntimeContext context,
      KeyedStateBackend<Object> keyedStateBackend,
      TimerServiceFactory timerServiceFactory,
      InternalListState<String, Long, Message> delayedMessagesBufferState,
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs,
      Output<StreamRecord<Message>> output,
      MessageFactory messageFactory,
      Executor mailboxExecutor,
      MetricGroup metricGroup,
      MapState<Long, Message> asyncOperations,
      Executor checkpointLockExecutor) {

    ObjectContainer container = new ObjectContainer();

    container.add("function-providers", Map.class, statefulFunctionsUniverse.functions());
    container.add(
        "function-repository", FunctionRepository.class, StatefulFunctionRepository.class);

    // for FlinkState
    container.add("runtime-context", RuntimeContext.class, context);
    container.add("keyed-state-backend", KeyedStateBackend.class, keyedStateBackend);
    container.add(new DynamicallyRegisteredTypes(statefulFunctionsUniverse.types()));

    if (useMultiplexedState(keyedStateBackend)) {
      container.add("state", State.class, MultiplexedState.class);
    } else {
      container.add("state", State.class, FlinkState.class);
    }

    // For reductions
    container.add(messageFactory);

    container.add(
        new Partition(
            context.getMaxNumberOfParallelSubtasks(),
            context.getNumberOfParallelSubtasks(),
            context.getIndexOfThisSubtask()));

    container.add(new RemoteSink(output));
    container.add(new SideOutputSink(sideOutputs, output));

    container.add("applying-context", ApplyingContext.class, ReusableContext.class);
    container.add(LocalSink.class);
    container.add("function-loader", FunctionLoader.class, PredefinedFunctionLoader.class);
    container.add(StateBinder.class);
    container.add(Reductions.class);
    container.add(LocalFunctionGroup.class);
    container.add("metrics-factory", MetricsFactory.class, new FlinkMetricsFactory(metricGroup));

    // for delayed messages
    container.add(
        "delayed-messages-buffer-state", InternalListState.class, delayedMessagesBufferState);
    container.add(
        "delayed-messages-buffer",
        DelayedMessagesBuffer.class,
        FlinkStateDelayedMessagesBuffer.class);
    container.add(
        "delayed-messages-timer-service-factory", TimerServiceFactory.class, timerServiceFactory);
    container.add(DelaySink.class);

    // lazy providers for the sinks
    container.add("function-group", new Lazy<>(LocalFunctionGroup.class));
    container.add("reductions", new Lazy<>(Reductions.class));

    container.add("mailbox-executor", Executor.class, mailboxExecutor);

    // for the async operations
    container.add("async-operations", MapState.class, asyncOperations);
    container.add("checkpoint-lock-executor", Executor.class, checkpointLockExecutor);
    container.add(AsyncSink.class);

    return container.get(Reductions.class);
  }

  void apply(Message message) {
    enqueue(message);
    processEnvelopes();
  }

  void enqueue(Message message) {
    localFunctionGroup.enqueue(message);
  }

  @SuppressWarnings("StatementWithEmptyBody")
  void processEnvelopes() {
    while (localFunctionGroup.processNextEnvelope()) {
      // TODO: consider preemption if too many local messages.
    }
  }

  private static boolean useMultiplexedState(KeyedStateBackend<?> keyedStateBackend) {
    final String backendClassName = keyedStateBackend.getClass().getName();

    // TODO this is fragile and error-prone to classname changes, but we're doing this
    // TODO to avoid additional dependencies on the Flink state backends
    // TODO ideally, we should revisit how configuration is being passed to the
    // TODO operators to be available at runtime
    if (backendClassName.equals("org.apache.flink.runtime.state.heap.HeapKeyedStateBackend")) {
      return false;
    }
    if (backendClassName.equals(
        "org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend")) {
      return true;
    }
    throw new IllegalStateException("Unrecognized state backend type: " + backendClassName);
  }
}
