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
package com.ververica.statefun.flink.core.feedback;

import com.ververica.statefun.flink.core.common.SerializableFunction;
import com.ververica.statefun.flink.core.common.SerializablePredicate;
import com.ververica.statefun.flink.core.logger.Loggers;
import com.ververica.statefun.flink.core.logger.UnboundedFeedbackLogger;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.IOUtils;

public final class FeedbackUnionOperator<T> extends AbstractStreamOperator<T>
    implements FeedbackConsumer<T>, OneInputStreamOperator<T, T> {

  private static final long serialVersionUID = 1L;

  // -- configuration
  private final FeedbackKey<T> feedbackKey;
  private final SerializablePredicate<T> isBarrierMessage;
  private final SerializableFunction<T, ?> keySelector;
  private final long totalMemoryUsedForFeedbackCheckpointing;
  private final TypeSerializer<T> elementSerializer;

  // -- runtime
  private transient UnboundedFeedbackLogger<T> feedbackLogger;
  private transient boolean closedOrDisposed;
  private transient MailboxExecutor mailboxExecutor;
  private transient StreamRecord<T> reusable;

  FeedbackUnionOperator(
      FeedbackKey<T> feedbackKey,
      SerializablePredicate<T> isBarrierMessage,
      SerializableFunction<T, ?> keySelector,
      long totalMemoryUsedForFeedbackCheckpointing,
      TypeSerializer<T> elementTypeInformation,
      MailboxExecutor mailboxExecutor) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.isBarrierMessage = Objects.requireNonNull(isBarrierMessage);
    this.keySelector = Objects.requireNonNull(keySelector);
    this.totalMemoryUsedForFeedbackCheckpointing = totalMemoryUsedForFeedbackCheckpointing;
    this.elementSerializer = Objects.requireNonNull(elementTypeInformation);
    this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
    this.chainingStrategy = ChainingStrategy.ALWAYS;
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
      // since this code executes on a different thread than the operator thread
      // (although using the same checkpoint lock), we must check if the operator
      // wasn't closed or disposed.
      return;
    }
    if (isBarrierMessage.test(element)) {
      feedbackLogger.commit();
    } else {
      sendDownstream(element);
      feedbackLogger.append(element);
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
    UnboundedFeedbackLogger<T> feedbackLogger =
        (UnboundedFeedbackLogger<T>)
            Loggers.unboundedSpillableLogger(
                ioManager,
                maxParallelism,
                totalMemoryUsedForFeedbackCheckpointing,
                elementSerializer,
                keySelector);

    this.feedbackLogger = feedbackLogger;
    //
    // we first must reply previously check-pointed envelopes before we start
    // processing any new envelopes.
    //
    for (KeyGroupStatePartitionStreamProvider keyedStateInput : context.getRawKeyedStateInputs()) {
      this.feedbackLogger.replyLoggedEnvelops(keyedStateInput.getStream(), this);
    }
    //
    // now we can start processing new messages. We do so by registering ourselfs as a
    // FeedbackConsumer
    //
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
    final SubtaskFeedbackKey<T> key =
        feedbackKey.withSubTaskIndex(getRuntimeContext().getIndexOfThisSubtask());
    final StreamTask<?, ?> containingTask = getContainingTask();
    Feedback.registerFeedbackConsumer(key, containingTask, mailboxExecutor, this);
  }

  private void sendDownstream(T element) {
    reusable.replace(element);
    output.collect(reusable);
  }
}
