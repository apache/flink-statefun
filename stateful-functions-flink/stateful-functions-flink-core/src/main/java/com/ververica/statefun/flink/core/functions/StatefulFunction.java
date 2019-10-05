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

import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.metrics.FunctionTypeMetrics;
import com.ververica.statefun.flink.core.state.BoundState;
import com.ververica.statefun.sdk.Context;
import java.util.Objects;
import java.util.Optional;

final class StatefulFunction implements LiveFunction {
  private final com.ververica.statefun.sdk.StatefulFunction statefulFunction;
  private final BoundState state;
  private final FunctionTypeMetrics metrics;
  private final MessageFactory messageFactory;

  StatefulFunction(
      com.ververica.statefun.sdk.StatefulFunction statefulFunction,
      BoundState state,
      FunctionTypeMetrics metrics,
      MessageFactory messageFactory) {

    this.statefulFunction = Objects.requireNonNull(statefulFunction);
    this.state = Objects.requireNonNull(state);
    this.metrics = Objects.requireNonNull(metrics);
    this.messageFactory = Objects.requireNonNull(messageFactory);
  }

  @Override
  public void receive(Context context, Message message) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader targetClassLoader = statefulFunction.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(targetClassLoader);
      Object payload = message.payload(messageFactory, targetClassLoader);
      statefulFunction.invoke(context, payload);
    } catch (Exception e) {
      throw new StatefulFunctionInvocationException(context.self().type(), e);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Override
  public FunctionTypeMetrics metrics() {
    return metrics;
  }

  @Override
  public Optional<BoundState> state() {
    return Optional.of(state);
  }
}
