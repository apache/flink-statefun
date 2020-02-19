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
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;

public final class FunctionGroupDispatchFactory
    implements OneInputStreamOperatorFactory<Message, Message>, YieldingOperatorFactory<Message> {

  private static final long serialVersionUID = 1;

  private final StatefulFunctionsConfig configuration;

  private final Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs;

  private transient MailboxExecutor mailboxExecutor;

  public FunctionGroupDispatchFactory(
      StatefulFunctionsConfig configuration,
      Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs) {
    this.configuration = configuration;
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

    FunctionGroupOperator fn =
        new FunctionGroupOperator(
            sideOutputs, configuration, mailboxExecutor, ChainingStrategy.ALWAYS);
    fn.setup(containingTask, config, output);

    return (T) fn;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    // We ignore the chaining strategy, because we only use ChainingStrategy.ALWAYS
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return ChainingStrategy.ALWAYS;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return FunctionGroupOperator.class;
  }
}
