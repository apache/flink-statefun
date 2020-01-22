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

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class ReadySet {
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Condition hasWork = lock.newCondition();
  private final HashSet<String> enqueuedIds = new HashSet<>();
  private final ArrayDeque<Task> ready = new ArrayDeque<>(4096);

  void add(Task e) {
    Objects.requireNonNull(e);
    lock.lock();
    try {
      if (!enqueuedIds.add(e.id())) {
        return;
      }
      ready.addLast(e);
      hasWork.signalAll();
    } finally {
      lock.unlock();
    }
  }

  Task take() throws InterruptedException {
    lock.lock();
    try {
      while (ready.isEmpty()) {
        hasWork.await();
      }
      final Task e = ready.poll();
      enqueuedIds.remove(e.id());
      return e;
    } finally {
      lock.unlock();
    }
  }
}
