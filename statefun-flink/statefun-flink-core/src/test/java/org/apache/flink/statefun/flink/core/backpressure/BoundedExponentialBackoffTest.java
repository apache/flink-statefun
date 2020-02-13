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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import org.junit.Test;

public class BoundedExponentialBackoffTest {
  private final FakeNanoClock fakeTime = new FakeNanoClock();
  private final BoundedExponentialBackoff backoffUnderTest =
      new BoundedExponentialBackoff(fakeTime, Duration.ofSeconds(1), Duration.ofMinutes(1));

  @Test
  public void simpleUsage() {
    assertThat(backoffUnderTest.applyNow(), is(true));
    assertThat(fakeTime.now(), greaterThan(0L));
  }

  @Test
  public void timeoutExpired() {
    fakeTime.now = Duration.ofMinutes(1).toNanos();
    assertThat(backoffUnderTest.applyNow(), is(false));
  }

  @Test
  @SuppressWarnings("StatementWithEmptyBody")
  public void totalNumberOfBackoffsIsEqualToTimeout() {
    while (backoffUnderTest.applyNow()) {}

    assertThat(fakeTime.now(), is(Duration.ofMinutes(1).toNanos()));
  }

  private static final class FakeNanoClock implements Timer {
    long now;

    @Override
    public long now() {
      return now;
    }

    @Override
    public void sleep(long durationNano) {
      now += durationNano;
    }
  }
}
