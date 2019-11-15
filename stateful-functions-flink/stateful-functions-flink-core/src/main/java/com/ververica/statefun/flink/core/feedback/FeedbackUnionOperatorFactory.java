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
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

public final class FeedbackUnionOperatorFactory<E>
    implements OneInputStreamOperatorFactory<E, E>, YieldingOperatorFactory<E> {

  private static final long serialVersionUID = 1;

  private final FeedbackKey<E> feedbackKey;
  private final SerializablePredicate<E> isBarrierMessage;
  private final SerializableFunction<E, ?> keySelector;

  private transient MailboxExecutor mailboxExecutor;
  private transient ChainingStrategy chainingStrategy;

  public FeedbackUnionOperatorFactory(
      FeedbackKey<E> feedbackKey,
      SerializablePredicate<E> isBarrierMessage,
      SerializableFunction<E, ?> keySelector) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.isBarrierMessage = Objects.requireNonNull(isBarrierMessage);
    this.keySelector = Objects.requireNonNull(keySelector);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<E>> T createStreamOperator(
      StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<E>> output) {
    final TypeSerializer<E> serializer =
        config.getTypeSerializerIn1(containingTask.getUserCodeClassLoader());

    final long totalMemoryUsedForFeedbackCheckpointing =
        config
            .getConfiguration()
            .getInteger(FeedbackConfiguration.TOTAL_MEMORY_USED_FOR_FEEDBACK_CHECKPOINTING);

    FeedbackUnionOperator<E> op =
        new FeedbackUnionOperator<>(
            feedbackKey,
            isBarrierMessage,
            keySelector,
            totalMemoryUsedForFeedbackCheckpointing,
            serializer,
            mailboxExecutor);

    op.setup(containingTask, config, output);

    return (T) op;
  }

  @Override
  public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
    this.mailboxExecutor =
        Objects.requireNonNull(mailboxExecutor, "Mailbox executor can't be NULL");
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    // ignored
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return ChainingStrategy.ALWAYS;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return FeedbackUnionOperator.class;
  }
}
