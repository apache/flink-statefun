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

import com.ververica.statefun.flink.core.feedback.FeedbackKey;
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;

public final class FunctionGroupDispatchFactory
    implements OneInputStreamOperatorFactory<Message, Message>, YieldingOperatorFactory<Message> {

  private static final long serialVersionUID = 1;

  private final FeedbackKey<Message> feedbackKey;
  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;

  private transient MailboxExecutor mailboxExecutor;
  private transient ChainingStrategy chainingStrategy;

  public FunctionGroupDispatchFactory(
      FeedbackKey<Message> feedbackKey, Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs) {
    this.feedbackKey = feedbackKey;
    this.sideOutputs = sideOutputs;
  }

  @Override
  public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
    this.mailboxExecutor =
        Objects.requireNonNull(mailboxExecutor, "Mailbox executor can't be NULL");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<Message>> T createStreamOperator(
      StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Message>> output) {

    FunctionGroupOperator fn = new FunctionGroupOperator(feedbackKey, sideOutputs, mailboxExecutor);
    fn.setup(containingTask, config, output);

    return (T) fn;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    this.chainingStrategy = chainingStrategy;
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return chainingStrategy;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return FunctionGroupOperator.class;
  }
}
