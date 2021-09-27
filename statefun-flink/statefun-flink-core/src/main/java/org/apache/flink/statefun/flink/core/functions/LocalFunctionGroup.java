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

import java.util.ArrayDeque;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.FunctionType;

final class LocalFunctionGroup {
  private final FunctionRepository repository;
  private final ApplyingContext context;
  /**
   * pending is a queue of pairs (LiveFunction, Message) as enqueued via {@link #enqueue(Message)}.
   * In order to avoid an object pool, or redundant allocations we store these pairs linearly one
   * after another in the queue.
   */
  private final ArrayDeque<Object> pending;

  @Inject
  LocalFunctionGroup(
      @Label("function-repository") FunctionRepository repository,
      @Label("applying-context") ApplyingContext context) {
    this.pending = new ArrayDeque<>(4096);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
  }

  void enqueue(Message message) {
    FunctionType targetType = message.target().type();
    LiveFunction fn = repository.get(targetType);
    pending.addLast(fn);
    pending.addLast(message);
  }

  boolean processNextEnvelope() {
    Object fn = pending.pollFirst();
    if (fn == null) {
      return false;
    }
    LiveFunction liveFunction = (LiveFunction) fn;
    Message message = (Message) pending.pollFirst();
    context.apply(liveFunction, message);
    return true;
  }
}
