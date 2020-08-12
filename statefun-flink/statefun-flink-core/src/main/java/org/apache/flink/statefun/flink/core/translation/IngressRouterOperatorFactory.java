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
package org.apache.flink.statefun.flink.core.translation;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverses;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class IngressRouterOperatorFactory
    implements OneInputStreamOperatorFactory<Object, Message> {

  private static final long serialVersionUID = 1L;

  private final StatefulFunctionsConfig configuration;

  private final IngressIdentifier<Object> identifier;

  private ChainingStrategy strategy = ChainingStrategy.ALWAYS;

  IngressRouterOperatorFactory(
      StatefulFunctionsConfig configuration, IngressIdentifier<Object> identifier) {
    this.configuration = Objects.requireNonNull(configuration);
    this.identifier = Objects.requireNonNull(identifier);
  }

  @Override
  public <T extends StreamOperator<Message>> T createStreamOperator(
      StreamOperatorParameters<Message> parameters) {
    StatefulFunctionsUniverse universe =
        StatefulFunctionsUniverses.get(
            parameters.getContainingTask().getUserCodeClassLoader(), configuration);
    IngressRouterOperator<Object> router = new IngressRouterOperator<>(universe, identifier);
    router.setup(
        parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    return (T) router;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy strategy) {
    this.strategy = strategy;
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return strategy;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return IngressRouterOperator.class;
  }
}
