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

package org.apache.flink.statefun.e2e.smoke.embedded;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Creates {@link CompletableFuture}s that can be completed successfully or unsuccessfully, within 1
 * millisecond delay.
 */
final class AsyncCompleter {

  private static final Throwable EXCEPTION;

  static {
    Throwable t = new RuntimeException();
    t.setStackTrace(new StackTraceElement[0]);
    EXCEPTION = t;
  }

  private static final class Task {
    final long time;
    final CompletableFuture<Boolean> future;
    final boolean success;

    public Task(boolean success) {
      this.time = System.nanoTime();
      this.future = new CompletableFuture<>();
      this.success = success;
    }
  }

  private static final int ONE_MILLISECOND = Duration.ofMillis(1).getNano();
  private final LinkedBlockingDeque<Task> queue = new LinkedBlockingDeque<>();
  private boolean started;

  /**
   * Returns a future that would be complete successfully, no sooner than 1 millisecond from now.
   */
  CompletableFuture<Boolean> successfulFuture() {
    return future(true);
  }

  /**
   * Returns a future that would be completed unsuccessfully, no sooner than 1 millisecond from now.
   */
  CompletableFuture<Boolean> failedFuture() {
    return future(false);
  }

  private CompletableFuture<Boolean> future(boolean success) {
    Task e = new Task(success);
    queue.add(e);
    return e.future;
  }

  void start() {
    if (started) {
      return;
    }
    started = true;
    Thread t = new Thread(this::run);
    t.setDaemon(true);
    t.start();
  }

  @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
  void run() {
    while (true) {
      try {
        Task e = queue.take();
        final long duration = System.nanoTime() - e.time;
        if (duration < ONE_MILLISECOND) {
          Thread.sleep(1);
        }
        CompletableFuture<Boolean> future = e.future;
        if (e.success) {
          future.complete(Boolean.TRUE);
        } else {
          future.completeExceptionally(EXCEPTION);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
