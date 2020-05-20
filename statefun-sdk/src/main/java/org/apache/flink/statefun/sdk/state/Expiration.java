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

package org.apache.flink.statefun.sdk.state;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;

/**
 * State Expiration Configuration
 *
 * <p>This class defines the way state can be auto expired by the runtime. State expiration (also
 * known as state TTL) can be used to keep state from growing arbitrarily by assigning an expiration
 * date to a {@link PersistedAppendingBuffer}, {@link PersistedValue} or a {@link PersistedTable}.
 *
 * <p>State can be expired after a duration had passed since either from the last write to the
 * state, or the last read.
 */
public final class Expiration {
  public enum Mode {
    NONE,
    AFTER_WRITE,
    AFTER_READ_OR_WRITE;
  }

  /**
   * Returns an Expiration configuration that would expire a @duration after the last write.
   *
   * @param duration a duration to wait before considering the state expired.
   */
  public static Expiration expireAfterWriting(Duration duration) {
    return new Expiration(Mode.AFTER_WRITE, duration);
  }

  /**
   * Returns an Expiration configuration that would expire a @duration after the last write or read.
   *
   * @param duration a duration to wait before considering the state expired.
   */
  public static Expiration expireAfterReadingOrWriting(Duration duration) {
    return new Expiration(Mode.AFTER_READ_OR_WRITE, duration);
  }

  public static Expiration expireAfter(Duration duration, Mode mode) {
    return new Expiration(mode, duration);
  }

  /** @return Returns a disabled expiration */
  public static Expiration none() {
    return new Expiration(Mode.NONE, Duration.ZERO);
  }

  private final Mode mode;
  private final Duration duration;

  @ForRuntime
  public Expiration(Mode mode, Duration duration) {
    this.mode = Objects.requireNonNull(mode);
    this.duration = Objects.requireNonNull(duration);
  }

  public Mode mode() {
    return mode;
  }

  public Duration duration() {
    return duration;
  }
}
