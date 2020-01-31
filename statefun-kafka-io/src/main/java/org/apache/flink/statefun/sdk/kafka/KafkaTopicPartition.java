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

import java.util.Objects;

/** Representation of a Kafka partition. */
public final class KafkaTopicPartition {

  private final String topic;
  private final int partition;

  public static KafkaTopicPartition fromString(String topicAndPartition) {
    Objects.requireNonNull(topicAndPartition);
    final int pos = topicAndPartition.lastIndexOf("/");
    if (pos <= 0 || pos == topicAndPartition.length() - 1) {
      throw new IllegalArgumentException(
          topicAndPartition + " does not conform to the <topic>/<partition_id> format");
    }

    String topic = topicAndPartition.substring(0, pos);
    Integer partitionId;
    try {
      partitionId = Integer.valueOf(topicAndPartition.substring(pos + 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid topic partition definition: "
              + topicAndPartition
              + "; partition id is expected to be an integer with value between 0 and "
              + Integer.MAX_VALUE,
          e);
    }

    if (partitionId < 0) {
      throw new IllegalArgumentException(
          "Invalid topic partition definition: "
              + topicAndPartition
              + "; partition id is expected to be an integer with value between 0 and "
              + Integer.MAX_VALUE);
    }

    return new KafkaTopicPartition(topic, partitionId);
  }

  public KafkaTopicPartition(String topic, int partition) {
    this.topic = Objects.requireNonNull(topic);

    if (partition < 0) {
      throw new IllegalArgumentException(
          "Invalid partition id: " + partition + "; value must be larger or equal to 0.");
    }
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  @Override
  public String toString() {
    return "KafkaTopicPartition{" + "topic='" + topic + '\'' + ", partition=" + partition + '}';
  }

  @Override
  public int hashCode() {
    return 31 * topic.hashCode() + partition;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o == this) {
      return true;
    }

    if (!(o instanceof KafkaTopicPartition)) {
      return false;
    }

    KafkaTopicPartition that = (KafkaTopicPartition) o;
    return this.partition == that.partition && this.topic.equals(that.topic);
  }
}
