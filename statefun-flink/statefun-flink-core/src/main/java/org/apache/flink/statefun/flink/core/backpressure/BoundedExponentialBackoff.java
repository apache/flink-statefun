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

import static java.lang.Math.random;
import static java.lang.Math.round;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BoundedExponentialBackoff {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedExponentialBackoff.class);

  private final Timer timer;
  private final long requestStartTimeInNanos;
  private final long maxRequestDurationInNanos;
  private final double backOffIncreaseFactor;
  private final double jitter;

  private long nextSleepTimeNanos;

  public BoundedExponentialBackoff(
      Duration initialBackoffDuration,
      double backOffIncreaseFactor,
      double jitter,
      Duration maxRequestDuration) {
    this(
        SystemNanoTimer.instance(),
        initialBackoffDuration,
        backOffIncreaseFactor,
        jitter,
        maxRequestDuration);
  }

  @VisibleForTesting
  BoundedExponentialBackoff(
      Timer timer,
      Duration initialBackoffDuration,
      double backOffIncreaseFactor,
      double jitter,
      Duration maxRequestDuration) {
    this.timer = Objects.requireNonNull(timer);
    this.requestStartTimeInNanos = timer.now();
    this.backOffIncreaseFactor = backOffIncreaseFactor;
    this.jitter = jitter;
    this.maxRequestDurationInNanos = maxRequestDuration.toNanos();
    this.nextSleepTimeNanos = initialBackoffDuration.toNanos();
    this.nextSleepTimeNanos =
        round(initialBackoffDuration.toNanos() * (1.0 + jitter * (-1.0 + 2.0 * random())));
  }

  public boolean applyNow() {
    final long remainingNanos = remainingNanosUntilDeadLine();
    final long nextAmountOfNanosToSleep = nextAmountOfNanosToSleep();
    final long actualSleep = Math.min(remainingNanos, nextAmountOfNanosToSleep);
    if (actualSleep <= 0) {
      return false;
    }

    LOG.info(
        String.format(
            "Applying exponential backoff. Sleeping for %f seconds.",
            actualSleep * Math.pow(10, -9)));

    timer.sleep(actualSleep);
    return true;
  }

  private long remainingNanosUntilDeadLine() {
    final long totalElapsedTime = timer.now() - requestStartTimeInNanos;
    return maxRequestDurationInNanos - totalElapsedTime;
  }

  private long nextAmountOfNanosToSleep() {
    final long current = nextSleepTimeNanos;
    nextSleepTimeNanos =
        round(nextSleepTimeNanos * backOffIncreaseFactor * (1 + jitter * (-1 + 2 * random())));
    return current;
  }
}
