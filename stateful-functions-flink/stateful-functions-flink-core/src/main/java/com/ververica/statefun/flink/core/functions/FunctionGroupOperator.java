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

import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverses;
import com.ververica.statefun.flink.core.common.MailboxExecutorFacade;
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

public class FunctionGroupOperator extends AbstractStreamOperator<Message>
    implements OneInputStreamOperator<Message, Message> {

  private static final long serialVersionUID = 1L;

  // -- configuration
  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;

  // -- runtime
  private transient Reductions reductions;
  private transient boolean closedOrDisposed;
  private transient MailboxExecutor mailboxExecutor;

  FunctionGroupOperator(
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs,
      MailboxExecutor mailboxExecutor,
      ChainingStrategy chainingStrategy) {
    this.sideOutputs = Objects.requireNonNull(sideOutputs);
    this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
    this.chainingStrategy = chainingStrategy;
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Operator API
  // ------------------------------------------------------------------------------------------------------------------

  @Override
  public void processElement(StreamRecord<Message> record) {
    reductions.apply(record.getValue());
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    final Configuration configuration = getConfiguration();
    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        statefulFunctionsUniverse(configuration);

    final TypeSerializer<Message> envelopeSerializer =
        getOperatorConfig().getTypeSerializerIn1(getContainingTask().getUserCodeClassLoader());
    final Executor checkpointLockExecutor =
        new UnderCheckpointLockExecutor(
            getContainingTask().getCheckpointLock(), () -> closedOrDisposed, getContainingTask());
    final MapStateDescriptor<Long, Message> asyncOperationStateDescriptor =
        new MapStateDescriptor<>(
            "asyncOperations", LongSerializer.INSTANCE, envelopeSerializer.duplicate());
    final ListStateDescriptor<Message> delayedMessageStateDescriptor =
        new ListStateDescriptor<>(
            FlinkStateDelayedMessagesBuffer.BUFFER_STATE_NAME, envelopeSerializer.duplicate());
    final MapState<Long, Message> asyncOperationState =
        getRuntimeContext().getMapState(asyncOperationStateDescriptor);

    Objects.requireNonNull(mailboxExecutor, "MailboxExecutor is unexpectedly NULL");

    //
    // the core logic of applying messages to functions.
    //
    this.reductions =
        Reductions.create(
            configuration,
            statefulFunctionsUniverse,
            getRuntimeContext(),
            getKeyedStateBackend(),
            new FlinkTimerServiceFactory(super.timeServiceManager),
            delayedMessagesBufferState(delayedMessageStateDescriptor),
            sideOutputs,
            output,
            MessageFactory.forType(statefulFunctionsUniverse.messageFactoryType()),
            new MailboxExecutorFacade(mailboxExecutor, "Stateful Functions Mailbox"),
            getRuntimeContext().getMetricGroup().addGroup("functions"),
            asyncOperationState,
            checkpointLockExecutor);
    //
    // expire all the pending async operations.
    //
    AsyncOperationFailureNotifier.fireExpiredAsyncOperations(
        asyncOperationStateDescriptor, reductions, asyncOperationState, getKeyedStateBackend());
  }

  @Override
  public void close() throws Exception {
    closeInternally();
    super.close();
  }

  @Override
  public void dispose() throws Exception {
    closeInternally();
    super.dispose();
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------------------------------------

  private void closeInternally() {
    closedOrDisposed = true;
  }

  private Configuration getConfiguration() {
    Configuration merged = new Configuration();

    merged.addAll(getContainingTask().getJobConfiguration());

    GlobalJobParameters globalJobParameters =
        getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    Preconditions.checkState(globalJobParameters instanceof Configuration);
    Configuration configuration = (Configuration) globalJobParameters;

    merged.addAll(configuration);
    return merged;
  }

  private InternalListState<String, Long, Message> delayedMessagesBufferState(
      ListStateDescriptor<Message> delayedMessageStateDescriptor) {
    try {
      KeyedStateBackend<String> keyedStateBackend = getKeyedStateBackend();
      return (InternalListState<String, Long, Message>)
          keyedStateBackend.getOrCreateKeyedState(
              LongSerializer.INSTANCE, delayedMessageStateDescriptor);
    } catch (Exception e) {
      throw new RuntimeException("Error registered Flink state for delayed messages buffer.", e);
    }
  }

  private StatefulFunctionsUniverse statefulFunctionsUniverse(Configuration configuration) {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return StatefulFunctionsUniverses.get(classLoader, configuration);
  }
}
