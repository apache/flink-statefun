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
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.time.ZonedDateTime;

/** Position for the ingress to start consuming AWS Kinesis shards. */
public abstract class KinesisIngressStartupPosition {

  private KinesisIngressStartupPosition() {}

  /** Start consuming from the earliest position possible. */
  public static KinesisIngressStartupPosition fromEarliest() {
    return EarliestPosition.INSTANCE;
  }

  /** Start consuming from the latest position, i.e. head of the stream shards. */
  public static KinesisIngressStartupPosition fromLatest() {
    return LatestPosition.INSTANCE;
  }

  /**
   * Start consuming from position with ingestion timestamps after or equal to a specified {@link
   * ZonedDateTime}.
   */
  public static KinesisIngressStartupPosition fromDate(ZonedDateTime date) {
    return new DatePosition(date);
  }

  /** Checks whether this position is configured using the earliest position. */
  public final boolean isEarliest() {
    return getClass() == EarliestPosition.class;
  }

  /** Checks whether this position is configured using the latest position. */
  public final boolean isLatest() {
    return getClass() == LatestPosition.class;
  }

  /** Checks whether this position is configured using a date. */
  public final boolean isDate() {
    return getClass() == DatePosition.class;
  }

  /** Returns this position as a {@link DatePosition}. */
  public final DatePosition asDate() {
    if (!isDate()) {
      throw new IllegalStateException("This is not a startup position configured using a date.");
    }
    return (DatePosition) this;
  }

  @SuppressWarnings("WeakerAccess")
  public static final class EarliestPosition extends KinesisIngressStartupPosition {
    private static final EarliestPosition INSTANCE = new EarliestPosition();
  }

  @SuppressWarnings("WeakerAccess")
  public static final class LatestPosition extends KinesisIngressStartupPosition {
    private static final LatestPosition INSTANCE = new LatestPosition();
  }

  public static final class DatePosition extends KinesisIngressStartupPosition {

    private final ZonedDateTime date;

    private DatePosition(ZonedDateTime date) {
      this.date = date;
    }

    public ZonedDateTime date() {
      return date;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof DatePosition)) {
        return false;
      }

      DatePosition that = (DatePosition) obj;
      return that.date.equals(date);
    }

    @Override
    public int hashCode() {
      return date.hashCode();
    }
  }
}
