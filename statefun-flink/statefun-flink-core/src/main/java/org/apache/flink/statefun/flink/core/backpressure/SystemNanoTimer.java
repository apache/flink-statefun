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

package org.apache.flink.statefun.flink.core.backpressure;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/** A {@code Timer} backed by {@link System#nanoTime()}. */
final class SystemNanoTimer implements Timer {
  private static final SystemNanoTimer INSTANCE = new SystemNanoTimer();

  public static SystemNanoTimer instance() {
    return INSTANCE;
  }

  private SystemNanoTimer() {}

  @Override
  public long now() {
    return System.nanoTime();
  }

  @Override
  public void sleep(long sleepTimeNanos) {
    try {
      final long sleepTimeMs = NANOSECONDS.toMillis(sleepTimeNanos);
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException ex) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while sleeping", ex);
      }
    }
  }
}
