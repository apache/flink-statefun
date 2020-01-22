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
package org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.annotation.Nullable;

final class Task {

  private final ConcurrentLinkedDeque<Object> events = new ConcurrentLinkedDeque<>();

  private final Simulatee simulatee;

  Task(Simulatee simulatee) {
    this.simulatee = Objects.requireNonNull(simulatee);
  }

  String id() {
    return simulatee.id();
  }

  final void enqueue(Object event) {
    Objects.requireNonNull(event);
    events.add(event);
  }

  final void processEnqueued() {
    final Simulatee simulatee = this.simulatee;
    while (!simulatee.isDone()) {
      final @Nullable Object event = events.poll();
      if (event == null) {
        return;
      }
      simulatee.apply(event);
    }
  }

  boolean isDone() {
    return simulatee.isDone();
  }

  boolean needReschedule() {
    return simulatee.needReschedule();
  }
}
