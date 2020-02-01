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
package org.apache.flink.statefun.sdk.kafka;

import java.time.ZonedDateTime;
import java.util.Map;

/** Position for the ingress to start consuming Kafka partitions. */
@SuppressWarnings("WeakerAccess, unused")
public class KafkaIngressStartupPosition {

  /** Private constructor to prevent instantiation. */
  private KafkaIngressStartupPosition() {}

  /**
   * Start consuming from committed consumer group offsets in Kafka.
   *
   * <p>Note that a consumer group id must be provided for this startup mode. Please see {@link
   * KafkaIngressBuilder#withConsumerGroupId(String)}.
   */
  public static KafkaIngressStartupPosition fromGroupOffsets() {
    return GroupOffsetsPosition.INSTANCE;
  }

  /** Start consuming from the earliest offset possible. */
  public static KafkaIngressStartupPosition fromEarliest() {
    return EarliestPosition.INSTANCE;
  }

  /** Start consuming from the latest offset, i.e. head of the topic partitions. */
  public static KafkaIngressStartupPosition fromLatest() {
    return LatestPosition.INSTANCE;
  }

  /**
   * Start consuming from a specified set of offsets.
   *
   * <p>If a specified offset does not exist for a partition, the position for that partition will
   * fallback to the reset position configured via {@link
   * KafkaIngressBuilder#withAutoResetPosition(KafkaIngressAutoResetPosition)}.
   *
   * @param specificOffsets map of specific set of offsets.
   */
  public static KafkaIngressStartupPosition fromSpecificOffsets(
      Map<KafkaTopicPartition, Long> specificOffsets) {
    if (specificOffsets == null || specificOffsets.isEmpty()) {
      throw new IllegalArgumentException("Provided specific offsets must not be empty.");
    }
    return new SpecificOffsetsPosition(specificOffsets);
  }

  /**
   * Start consuming from offsets with ingestion timestamps after or equal to a specified {@link
   * ZonedDateTime}.
   *
   * <p>If a Kafka partition does not have any records with ingestion timestamps after or equal to
   * the specified date, the position for that partition will fallback to the reset position
   * configured via {@link
   * KafkaIngressBuilder#withAutoResetPosition(KafkaIngressAutoResetPosition)}.
   */
  public static KafkaIngressStartupPosition fromDate(ZonedDateTime date) {
    return new DatePosition(date);
  }

  /** Checks whether this position is configured using committed consumer group offsets in Kafka. */
  public boolean isGroupOffsets() {
    return getClass() == GroupOffsetsPosition.class;
  }

  /** Checks whether this position is configured using the earliest offset. */
  public boolean isEarliest() {
    return getClass() == EarliestPosition.class;
  }

  /** Checks whether this position is configured using the latest offset. */
  public boolean isLatest() {
    return getClass() == LatestPosition.class;
  }

  /** Checks whether this position is configured using specific offsets. */
  public boolean isSpecificOffsets() {
    return getClass() == SpecificOffsetsPosition.class;
  }

  /** Checks whether this position is configured using a date. */
  public boolean isDate() {
    return getClass() == DatePosition.class;
  }

  /** Returns this position as a {@link SpecificOffsetsPosition}. */
  public SpecificOffsetsPosition asSpecificOffsets() {
    if (!isSpecificOffsets()) {
      throw new IllegalStateException(
          "This is not a startup position configured using specific offsets.");
    }
    return (SpecificOffsetsPosition) this;
  }

  /** Returns this position as a {@link DatePosition}. */
  public DatePosition asDate() {
    if (!isDate()) {
      throw new IllegalStateException("This is not a startup position configured using a Date.");
    }
    return (DatePosition) this;
  }

  public static final class GroupOffsetsPosition extends KafkaIngressStartupPosition {

    private static final GroupOffsetsPosition INSTANCE = new GroupOffsetsPosition();

    private GroupOffsetsPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof GroupOffsetsPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class EarliestPosition extends KafkaIngressStartupPosition {

    private static final EarliestPosition INSTANCE = new EarliestPosition();

    private EarliestPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof EarliestPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class LatestPosition extends KafkaIngressStartupPosition {

    private static final LatestPosition INSTANCE = new LatestPosition();

    private LatestPosition() {}

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof LatestPosition;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class SpecificOffsetsPosition extends KafkaIngressStartupPosition {

    private final Map<KafkaTopicPartition, Long> specificOffsets;

    private SpecificOffsetsPosition(Map<KafkaTopicPartition, Long> specificOffsets) {
      this.specificOffsets = specificOffsets;
    }

    public Map<KafkaTopicPartition, Long> getSpecificOffsets() {
      return specificOffsets;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof SpecificOffsetsPosition)) {
        return false;
      }

      SpecificOffsetsPosition that = (SpecificOffsetsPosition) obj;
      return that.specificOffsets.equals(specificOffsets);
    }

    @Override
    public int hashCode() {
      return specificOffsets.hashCode();
    }
  }

  public static final class DatePosition extends KafkaIngressStartupPosition {

    private final ZonedDateTime date;

    private DatePosition(ZonedDateTime date) {
      this.date = date;
    }

    public long getEpochMilli() {
      return date.toInstant().toEpochMilli();
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
