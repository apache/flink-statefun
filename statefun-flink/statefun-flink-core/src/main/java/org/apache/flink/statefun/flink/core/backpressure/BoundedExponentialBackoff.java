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

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;

public final class BoundedExponentialBackoff {
  private final Timer timer;
  private final long requestStartTimeInNanos;
  private final long maxRequestDurationInNanos;

  private long nextSleepTimeNanos;

  public BoundedExponentialBackoff(Duration initialBackoffDuration, Duration maxRequestDuration) {
    this(SystemNanoTimer.instance(), initialBackoffDuration, maxRequestDuration);
  }

  @VisibleForTesting
  BoundedExponentialBackoff(
      Timer timer, Duration initialBackoffDuration, Duration maxRequestDuration) {
    this.timer = Objects.requireNonNull(timer);
    this.requestStartTimeInNanos = timer.now();
    this.maxRequestDurationInNanos = maxRequestDuration.toNanos();
    this.nextSleepTimeNanos = initialBackoffDuration.toNanos();
  }

  public boolean applyNow() {
    final long remainingNanos = remainingNanosUntilDeadLine();
    final long nextAmountOfNanosToSleep = nextAmountOfNanosToSleep();
    final long actualSleep = Math.min(remainingNanos, nextAmountOfNanosToSleep);
    if (actualSleep <= 0) {
      return false;
    }
    timer.sleep(actualSleep);
    return true;
  }

  private long remainingNanosUntilDeadLine() {
    final long totalElapsedTime = timer.now() - requestStartTimeInNanos;
    return maxRequestDurationInNanos - totalElapsedTime;
  }

  private long nextAmountOfNanosToSleep() {
    final long current = nextSleepTimeNanos;
    nextSleepTimeNanos *= 2;
    return current;
  }
}
