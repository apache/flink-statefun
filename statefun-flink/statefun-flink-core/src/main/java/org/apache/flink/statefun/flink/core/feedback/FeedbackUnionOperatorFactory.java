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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.common.SerializableFunction;
import org.apache.flink.streaming.api.operators.*;

public final class FeedbackUnionOperatorFactory<E>
    implements OneInputStreamOperatorFactory<E, E>, YieldingOperatorFactory<E> {

  private static final long serialVersionUID = 1;

  private final StatefulFunctionsConfig configuration;

  private final FeedbackKey<E> feedbackKey;
  private final SerializableFunction<E, OptionalLong> isBarrierMessage;
  private final SerializableFunction<E, ?> keySelector;

  private transient MailboxExecutor mailboxExecutor;

  public FeedbackUnionOperatorFactory(
      StatefulFunctionsConfig configuration,
      FeedbackKey<E> feedbackKey,
      SerializableFunction<E, OptionalLong> isBarrierMessage,
      SerializableFunction<E, ?> keySelector) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.isBarrierMessage = Objects.requireNonNull(isBarrierMessage);
    this.keySelector = Objects.requireNonNull(keySelector);
    this.configuration = Objects.requireNonNull(configuration);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<E>> T createStreamOperator(
      StreamOperatorParameters<E> streamOperatorParameters) {
    final TypeSerializer<E> serializer =
        streamOperatorParameters
            .getStreamConfig()
            .getTypeSerializerIn(
                0, streamOperatorParameters.getContainingTask().getUserCodeClassLoader());

    FeedbackUnionOperator<E> op =
        new FeedbackUnionOperator<>(
            feedbackKey,
            isBarrierMessage,
            keySelector,
            configuration.getFeedbackBufferSize().getBytes(),
            serializer,
            mailboxExecutor,
            streamOperatorParameters.getProcessingTimeService());

    op.setup(
        streamOperatorParameters.getContainingTask(),
        streamOperatorParameters.getStreamConfig(),
        streamOperatorParameters.getOutput());

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
