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
package org.apache.flink.statefun.flink.state.processor.operator;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.state.api.output.SnapshotUtils;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.statefun.flink.core.functions.FunctionGroupOperator;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.state.FlinkState;
import org.apache.flink.statefun.flink.core.state.MultiplexedState;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.flink.core.types.DynamicallyRegisteredTypes;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.flink.state.processor.union.TaggedBootstrapData;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** An operator used to bootstrap function state for the {@link FunctionGroupOperator}. */
public final class FunctionsStateBootstrapOperator
    extends AbstractStreamOperator<TaggedOperatorSubtaskState>
    implements OneInputStreamOperator<TaggedBootstrapData, TaggedOperatorSubtaskState>,
        BoundedOneInput {

  private static final long serialVersionUID = 1L;

  private final StateBootstrapFunctionRegistry stateBootstrapFunctionRegistry;
  private final boolean disableMultiplexState;

  private final long snapshotTimestamp;
  private final Path snapshotPath;

  private transient StateBootstrapper stateBootstrapper;

  public FunctionsStateBootstrapOperator(
      StateBootstrapFunctionRegistry stateBootstrapFunctionRegistry,
      boolean disableMultiplexState,
      long snapshotTimestamp,
      Path snapshotPath) {
    this.stateBootstrapFunctionRegistry = stateBootstrapFunctionRegistry;
    this.disableMultiplexState = disableMultiplexState;
    this.snapshotTimestamp = snapshotTimestamp;
    this.snapshotPath = snapshotPath;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    final State stateAccessor =
        createStateAccessor(getRuntimeContext(), getKeyedStateBackend(), disableMultiplexState);
    this.stateBootstrapper = new StateBootstrapper(stateBootstrapFunctionRegistry, stateAccessor);
  }

  @Override
  public void processElement(StreamRecord<TaggedBootstrapData> streamRecord) throws Exception {
    stateBootstrapper.apply(streamRecord.getValue());
  }

  @Override
  public void endInput() throws Exception {
    // bootstrap dataset is now completely processed;
    // take a snapshot of the function states
    final TaggedOperatorSubtaskState state =
        SnapshotUtils.snapshot(
            this,
            getRuntimeContext().getIndexOfThisSubtask(),
            snapshotTimestamp,
            getContainingTask().getCheckpointStorage(),
            snapshotPath);

    output.collect(new StreamRecord<>(state));
  }

  private static State createStateAccessor(
      RuntimeContext runtimeContext,
      KeyedStateBackend<Object> keyedStateBackend,
      boolean disableMultiplexState) {
    if (disableMultiplexState) {
      return new FlinkState(
          runtimeContext,
          keyedStateBackend,
          new DynamicallyRegisteredTypes(
              new StaticallyRegisteredTypes(MessageFactoryType.WITH_RAW_PAYLOADS)));
    } else {
      return new MultiplexedState(
          runtimeContext,
          keyedStateBackend,
          new DynamicallyRegisteredTypes(
              new StaticallyRegisteredTypes(MessageFactoryType.WITH_RAW_PAYLOADS)));
    }
  }
}
