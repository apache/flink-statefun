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
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.statefun.flink.core.common.MailboxExecutorFacade;
import org.apache.flink.statefun.flink.core.common.SerializableFunction;
import org.apache.flink.statefun.flink.core.logger.Loggers;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLogger;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLoggerFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.IOUtils;

public final class FeedbackUnionOperator<T> extends AbstractStreamOperator<T>
    implements FeedbackConsumer<T>, OneInputStreamOperator<T, T> {

  private static final long serialVersionUID = 1L;

  // -- configuration
  private final FeedbackKey<T> feedbackKey;
  private final SerializableFunction<T, OptionalLong> isBarrierMessage;
  private final SerializableFunction<T, ?> keySelector;
  private final long totalMemoryUsedForFeedbackCheckpointing;
  private final TypeSerializer<T> elementSerializer;

  // -- runtime
  private transient Checkpoints<T> checkpoints;
  private transient boolean closedOrDisposed;
  private transient MailboxExecutor mailboxExecutor;
  private transient StreamRecord<T> reusable;

  FeedbackUnionOperator(
      FeedbackKey<T> feedbackKey,
      SerializableFunction<T, OptionalLong> isBarrierMessage,
      SerializableFunction<T, ?> keySelector,
      long totalMemoryUsedForFeedbackCheckpointing,
      TypeSerializer<T> elementSerializer,
      MailboxExecutor mailboxExecutor,
      ProcessingTimeService processingTimeService) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.isBarrierMessage = Objects.requireNonNull(isBarrierMessage);
    this.keySelector = Objects.requireNonNull(keySelector);
    this.totalMemoryUsedForFeedbackCheckpointing = totalMemoryUsedForFeedbackCheckpointing;
    this.elementSerializer = Objects.requireNonNull(elementSerializer);
    this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
    this.chainingStrategy = ChainingStrategy.ALWAYS;
    // Even though this operator does not use the processing
    // time service, AbstractStreamOperator requires this
    // field is non-null, otherwise we get a NullPointerException
    super.processingTimeService = processingTimeService;
  }

  // ------------------------------------------------------------------------------------------------------------------
  // API
  // ------------------------------------------------------------------------------------------------------------------

  @Override
  public void processElement(StreamRecord<T> streamRecord) {
    sendDownstream(streamRecord.getValue());
  }

  @Override
  public void processFeedback(T element) {
    if (closedOrDisposed) {
      return;
    }
    OptionalLong maybeCheckpoint = isBarrierMessage.apply(element);
    if (maybeCheckpoint.isPresent()) {
      checkpoints.commitCheckpointsUntil(maybeCheckpoint.getAsLong());
    } else {
      sendDownstream(element);
      checkpoints.append(element);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    final IOManager ioManager = getContainingTask().getEnvironment().getIOManager();
    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();

    this.reusable = new StreamRecord<>(null);

    //
    // Initialize the unbounded feedback logger
    //
    @SuppressWarnings("unchecked")
    UnboundedFeedbackLoggerFactory<T> feedbackLoggerFactory =
        (UnboundedFeedbackLoggerFactory<T>)
            Loggers.unboundedSpillableLoggerFactory(
                ioManager,
                maxParallelism,
                totalMemoryUsedForFeedbackCheckpointing,
                elementSerializer,
                keySelector);

    this.checkpoints = new Checkpoints<>(feedbackLoggerFactory::create);

    //
    // we first must reply previously check-pointed envelopes before we start
    // processing any new envelopes.
    //
    UnboundedFeedbackLogger<T> logger = feedbackLoggerFactory.create();
    for (KeyGroupStatePartitionStreamProvider keyedStateInput : context.getRawKeyedStateInputs()) {
      logger.replyLoggedEnvelops(keyedStateInput.getStream(), this);
    }
    //
    // now we can start processing new messages. We do so by registering ourselves as a
    // FeedbackConsumer
    //
    registerFeedbackConsumer(new MailboxExecutorFacade(mailboxExecutor, "Feedback Consumer"));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    checkpoints.startLogging(context.getCheckpointId(), context.getRawKeyedOperatorStateOutput());
  }

  @Override
  protected boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void close() throws Exception {
    closeInternally();
    super.close();
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------------------------------------

  private void closeInternally() {
    IOUtils.closeQuietly(checkpoints);
    checkpoints = null;
    closedOrDisposed = true;
  }

  private void registerFeedbackConsumer(Executor mailboxExecutor) {
    final int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    final int attemptNum = getRuntimeContext().getAttemptNumber();
    final SubtaskFeedbackKey<T> key = feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
    FeedbackChannelBroker broker = FeedbackChannelBroker.get();
    FeedbackChannel<T> channel = broker.getChannel(key);
    channel.registerConsumer(this, mailboxExecutor);
  }

  private void sendDownstream(T element) {
    reusable.replace(element);
    output.collect(reusable);
  }
}
