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

import java.util.Date;
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
    return new GroupOffsetsPosition();
  }

  /** Start consuming from the earliest offset possible. */
  public static KafkaIngressStartupPosition fromEarliest() {
    return new EarliestPosition();
  }

  /** Start consuming from the latest offset, i.e. head of the topic partitions. */
  public static KafkaIngressStartupPosition fromLatest() {
    return new LatestPosition();
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
   * Date}.
   *
   * <p>If a Kafka partition does not have any records with ingestion timestamps after or equal to
   * the specified date, the position for that partition will fallback to the reset position
   * configured via {@link
   * KafkaIngressBuilder#withAutoResetPosition(KafkaIngressAutoResetPosition)}.
   */
  public static KafkaIngressStartupPosition fromDate(Date date) {
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

  public static class GroupOffsetsPosition extends KafkaIngressStartupPosition {}

  public static class EarliestPosition extends KafkaIngressStartupPosition {}

  public static class LatestPosition extends KafkaIngressStartupPosition {}

  public static class SpecificOffsetsPosition extends KafkaIngressStartupPosition {

    private final Map<KafkaTopicPartition, Long> specificOffsets;

    SpecificOffsetsPosition(Map<KafkaTopicPartition, Long> specificOffsets) {
      this.specificOffsets = specificOffsets;
    }

    public Map<KafkaTopicPartition, Long> getSpecificOffsets() {
      return specificOffsets;
    }
  }

  public static class DatePosition extends KafkaIngressStartupPosition {

    private final Date date;

    DatePosition(Date date) {
      this.date = date;
    }

    public long getTime() {
      return date.getTime();
    }
  }
}
