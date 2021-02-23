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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.ArrayDeque;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.pool.SimplePool;
import org.apache.flink.statefun.sdk.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LocalFunctionGroup {
  private final ObjectOpenHashMap<Address, FunctionActivation> activeFunctions;
  private final ArrayDeque<FunctionActivation> pending;
  private final SimplePool<FunctionActivation> pool;
  private final FunctionRepository repository;
  private final ApplyingContext context;

  @Inject
  LocalFunctionGroup(
      @Label("function-repository") FunctionRepository repository,
      @Label("applying-context") ApplyingContext context) {
    this.activeFunctions = new ObjectOpenHashMap<>();
    this.pending = new ArrayDeque<>();
    this.pool = new SimplePool<>(FunctionActivation::new, 1024);
    this.repository = Objects.requireNonNull(repository);
    this.context = Objects.requireNonNull(context);
  }

  void enqueue(Message message) {
    FunctionActivation activation = activeFunctions.get(message.target());
    if (activation == null) {
      activation = newActivation(message.target());
      pending.addLast(activation);
    }
    activation.add(message);
  }

  boolean processNextEnvelope() {
    FunctionActivation activation = pending.pollFirst();
    if (activation == null) {
      return false;
    }
    activation.applyNextPendingEnvelope(context);
    if (activation.hasPendingEnvelope()) {
      pending.addLast(activation);
    } else {
      activeFunctions.remove(activation.self());
      activation.setFunction(null, null);
      pool.release(activation);
    }
    return true;
  }

  private FunctionActivation newActivation(Address self) {
    LiveFunction function = repository.get(self.type());
    FunctionActivation activation = pool.get();
    activation.setFunction(self, function);
    return activation;
  }

  public int getPendingSize(){
    return pending.size();
  }
}
