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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

final class EventLoop implements Runnable {
  private final ReadySet readySet = new ReadySet();
  private final ConcurrentHashMap<String, Task> globalTasks;
  private final ScheduledExecutorService timerExecutor =
      Executors.newScheduledThreadPool(1, DaemonThreadFactory.INSTANCE);

  EventLoop(ConcurrentHashMap<String, Task> globalTasks) {
    this.globalTasks = globalTasks;
  }

  void addToReadySet(Task task) {
    readySet.add(task);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  @Override
  public void run() {
    while (true) {
      try {
        processTask();
      } catch (Throwable ignored) {
      }
    }
  }

  private void processTask() throws InterruptedException {
    final Task task = readySet.take();
    try {
      task.processEnqueued();
      if (task.isDone()) {
        // this entity is done and can be removed from the system
        globalTasks.remove(task.id());
      } else if (task.needReschedule()) {
        scheduleLater(task);
      }
    } catch (Throwable ignored) {
    }
  }

  private void scheduleLater(Task e) {
    final int jitter = ThreadLocalRandom.current().nextInt(500, 1100);
    Objects.requireNonNull(e);
    timerExecutor.schedule(
        () -> {
          e.enqueue(LifecycleMessages.timeTick());
          readySet.add(e);
        },
        jitter,
        TimeUnit.MILLISECONDS);
  }
}
