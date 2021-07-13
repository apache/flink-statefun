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
package org.apache.flink.statefun.e2e.smoke;

import static org.apache.flink.statefun.e2e.smoke.generated.Command.Verify;
import static org.apache.flink.statefun.e2e.smoke.generated.Command.newBuilder;

import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.statefun.e2e.smoke.generated.Command;
import org.apache.flink.statefun.e2e.smoke.generated.Commands;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.e2e.smoke.generated.SourceSnapshot;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink Source that Emits {@link SourceCommand}s.
 *
 * <p>This source is configured by {@link ModuleParameters} and would generate random commands,
 * addressed to various functions. This source might also throw exceptions (kaboom) to simulate
 * failures.
 *
 * <p>After generating {@link ModuleParameters#getMessageCount()} messages, this source will switch
 * to {@code verification} step. At this step, it would keep sending (every 2 seconds) a {@link
 * Verify} command to every function indefinitely.
 */
final class CommandFlinkSource extends RichSourceFunction<TypedValue>
    implements CheckpointedFunction, CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(CommandFlinkSource.class);

  // ------------------------------------------------------------------------------------------------------------
  // Configuration
  // ------------------------------------------------------------------------------------------------------------

  private final ModuleParameters moduleParameters;

  // ------------------------------------------------------------------------------------------------------------
  // Runtime
  // ------------------------------------------------------------------------------------------------------------

  private transient ListState<SourceSnapshot> sourceSnapshotHandle;
  private transient FunctionStateTracker functionStateTracker;
  private transient int commandsSentSoFar;
  private transient int failuresSoFar;
  private transient boolean done;
  private transient boolean atLeastOneCheckpointCompleted;

  public CommandFlinkSource(ModuleParameters moduleParameters) {
    this.moduleParameters = Objects.requireNonNull(moduleParameters);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    OperatorStateStore store = context.getOperatorStateStore();
    sourceSnapshotHandle =
        store.getUnionListState(new ListStateDescriptor<>("snapshot", SourceSnapshot.class));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    SourceSnapshot sourceSnapshot =
        getOnlyElement(sourceSnapshotHandle.get(), SourceSnapshot.getDefaultInstance());
    functionStateTracker =
        new FunctionStateTracker(moduleParameters.getNumberOfFunctionInstances())
            .apply(sourceSnapshot.getTracker());
    commandsSentSoFar = sourceSnapshot.getCommandsSentSoFarHandle();
    failuresSoFar = sourceSnapshot.getFailuresGeneratedSoFar();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    sourceSnapshotHandle.clear();
    sourceSnapshotHandle.add(
        SourceSnapshot.newBuilder()
            .setCommandsSentSoFarHandle(commandsSentSoFar)
            .setTracker(functionStateTracker.snapshot())
            .setFailuresGeneratedSoFar(failuresSoFar)
            .build());

    if (commandsSentSoFar < moduleParameters.getMessageCount()) {
      double perCent = 100.0d * (commandsSentSoFar) / moduleParameters.getMessageCount();
      LOG.info(
          "Commands sent {} / {} ({} %)",
          commandsSentSoFar, moduleParameters.getMessageCount(), perCent);
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    atLeastOneCheckpointCompleted = true;
  }

  @Override
  public void cancel() {
    done = true;
  }

  // ------------------------------------------------------------------------------------------------------------
  // Generation
  // ------------------------------------------------------------------------------------------------------------

  @Override
  public void run(SourceContext<TypedValue> ctx) {
    generate(ctx);
    do {
      verify(ctx);
      snooze();
      synchronized (ctx.getCheckpointLock()) {
        if (done) {
          return;
        }
      }
    } while (true);
  }

  private void generate(SourceContext<TypedValue> ctx) {
    final int startPosition = this.commandsSentSoFar;
    final OptionalInt kaboomIndex =
        computeFailureIndex(startPosition, failuresSoFar, moduleParameters.getMaxFailures());
    if (kaboomIndex.isPresent()) {
      failuresSoFar++;
    }
    LOG.info(
        "starting at {}, kaboom at {}, total messages {}",
        startPosition,
        kaboomIndex,
        moduleParameters.getMessageCount());
    Supplier<SourceCommand> generator =
        new CommandGenerator(new JDKRandomGenerator(), moduleParameters);
    FunctionStateTracker functionStateTracker = this.functionStateTracker;
    for (int i = startPosition; i < moduleParameters.getMessageCount(); i++) {
      if (atLeastOneCheckpointCompleted && kaboomIndex.isPresent() && i >= kaboomIndex.getAsInt()) {
        throw new RuntimeException("KABOOM!!!");
      }
      SourceCommand command = generator.get();
      synchronized (ctx.getCheckpointLock()) {
        if (done) {
          return;
        }
        functionStateTracker.apply(command);
        ctx.collect(TypedValueUtil.packProtobufMessage(command));
        this.commandsSentSoFar = i;
      }
    }
  }

  private void verify(SourceContext<TypedValue> ctx) {
    FunctionStateTracker functionStateTracker = this.functionStateTracker;

    for (int i = 0; i < moduleParameters.getNumberOfFunctionInstances(); i++) {
      final long expected = functionStateTracker.stateOf(i);

      Command.Builder verify = newBuilder().setVerify(Verify.newBuilder().setExpected(expected));

      SourceCommand command =
          SourceCommand.newBuilder()
              .setTarget(i)
              .setCommands(Commands.newBuilder().addCommand(verify))
              .build();
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(TypedValueUtil.packProtobufMessage(command));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Utils
  // ---------------------------------------------------------------------------------------------------------------

  private OptionalInt computeFailureIndex(int startPosition, int failureSoFar, int maxFailures) {
    if (failureSoFar >= maxFailures) {
      return OptionalInt.empty();
    }
    if (startPosition >= moduleParameters.getMessageCount()) {
      return OptionalInt.empty();
    }
    int index =
        ThreadLocalRandom.current().nextInt(startPosition, moduleParameters.getMessageCount());
    return OptionalInt.of(index);
  }

  private static void snooze() {
    try {
      Thread.sleep(2_000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T getOnlyElement(Iterable<T> items, T def) {
    Iterator<T> it = items.iterator();
    if (!it.hasNext()) {
      return def;
    }
    T item = it.next();
    if (it.hasNext()) {
      throw new IllegalStateException("Iterable has additional elements");
    }
    return item;
  }
}
