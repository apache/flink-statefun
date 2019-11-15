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

import static com.ververica.statefun.flink.core.StatefulFunctionsJobConstants.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING;

import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverses;
import com.ververica.statefun.flink.core.common.MessageKeyGroupAssigner;
import com.ververica.statefun.flink.core.feedback.Feedback;
import com.ververica.statefun.flink.core.feedback.FeedbackConsumer;
import com.ververica.statefun.flink.core.feedback.FeedbackKey;
import com.ververica.statefun.flink.core.feedback.SubtaskFeedbackKey;
import com.ververica.statefun.flink.core.logger.Loggers;
import com.ververica.statefun.flink.core.logger.UnboundedFeedbackLogger;
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.message.MessageTypeInformation;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

public class FunctionGroupOperator extends AbstractStreamOperator<Message>
    implements FeedbackConsumer<Message>, OneInputStreamOperator<Message, Message> {

  private static final long serialVersionUID = 1L;

  // -- configuration
  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;
  private final FeedbackKey<Message> feedbackKey;

  // -- runtime
  private transient Reductions reductions;
  private transient UnboundedFeedbackLogger<Message> feedbackLogger;
  private transient boolean closedOrDisposed;
  private transient MailboxExecutor mailboxExecutor;

  FunctionGroupOperator(
      FeedbackKey<Message> feedbackKey,
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs,
      MailboxExecutor mailboxExecutor) {

    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.sideOutputs = Objects.requireNonNull(sideOutputs);
    this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Operator API
  // ------------------------------------------------------------------------------------------------------------------

  @Override
  public void processElement(StreamRecord<Message> record) {
    reductions.apply(record.getValue());
  }

  @Override
  public void processFeedback(Message message) {
    if (closedOrDisposed) {
      // since this code executes on a different thread than the operator thread
      // (although using the same checkpoint lock), we must check if the operator
      // wasn't closed or disposed.
      return;
    }
    if (message.isBarrierMessage()) {
      feedbackLogger.commit();
    } else {
      reductions.apply(message);
      feedbackLogger.append(message);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    final Configuration configuration = getConfiguration();
    final StatefulFunctionsUniverse statefulFunctionsUniverse =
        statefulFunctionsUniverse(configuration);
    final TypeSerializer<Message> envelopeSerializer =
        messageTypeSerializer(statefulFunctionsUniverse);
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
            mailboxExecutor.asExecutor("Stateful Functions Mailbox"),
            getRuntimeContext().getMetricGroup().addGroup("functions"),
            asyncOperationState,
            checkpointLockExecutor);
    //
    // setup the write back edge logger
    //
    final int totalMemoryUsedForFeedbackCheckpointing =
        configuration.getInteger(TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING);
    IOManager ioManager = getContainingTask().getEnvironment().getIOManager();

    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();

    @SuppressWarnings("unchecked")
    UnboundedFeedbackLogger<Message> feedbackLogger =
        (UnboundedFeedbackLogger<Message>)
            Loggers.unboundedSpillableLogger(
                ioManager,
                maxParallelism,
                totalMemoryUsedForFeedbackCheckpointing,
                envelopeSerializer.duplicate(),
                new MessageKeyGroupAssigner(maxParallelism));

    this.feedbackLogger = feedbackLogger;

    //
    // expire all the pending async operations.
    //
    AsyncOperationFailureNotifier.fireExpiredAsyncOperations(
        asyncOperationStateDescriptor, reductions, asyncOperationState, getKeyedStateBackend());

    // we first must reply previously checkpointed envelopes before we start
    // processing any new envelopes.
    //
    for (KeyGroupStatePartitionStreamProvider keyedStateInput : context.getRawKeyedStateInputs()) {
      this.feedbackLogger.replyLoggedEnvelops(keyedStateInput.getStream(), this);
    }
    registerFeedbackConsumer(mailboxExecutor.asExecutor("Feedback Consumer"));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    this.feedbackLogger.startLogging(context.getRawKeyedOperatorStateOutput());
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
    IOUtils.closeQuietly(feedbackLogger);
    feedbackLogger = null;
    closedOrDisposed = true;
  }

  private void registerFeedbackConsumer(Executor mailboxExecutor) {
    final SubtaskFeedbackKey<Message> key =
        feedbackKey.withSubTaskIndex(getRuntimeContext().getIndexOfThisSubtask());
    final StreamTask<?, ?> containingTask = getContainingTask();

    Feedback.registerFeedbackConsumer(key, containingTask, mailboxExecutor, this);
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

  private TypeSerializer<Message> messageTypeSerializer(
      StatefulFunctionsUniverse statefulFunctionsUniverse) {
    TypeInformation<Message> info =
        new MessageTypeInformation(statefulFunctionsUniverse.messageFactoryType());
    return info.createSerializer(getExecutionConfig());
  }
}
