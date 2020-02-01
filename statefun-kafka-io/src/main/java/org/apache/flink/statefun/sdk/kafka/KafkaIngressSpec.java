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

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaIngressSpec<T> implements IngressSpec<T> {
  private final Properties properties;
  private final List<String> topics;
  private final KafkaIngressDeserializer<T> deserializer;
  private final KafkaIngressStartupPosition startupPosition;
  private final IngressIdentifier<T> ingressIdentifier;

  KafkaIngressSpec(
      IngressIdentifier<T> id,
      Properties properties,
      List<String> topics,
      KafkaIngressDeserializer<T> deserializer,
      KafkaIngressStartupPosition startupPosition) {
    this.properties = requireValidProperties(properties);
    this.topics = requireValidTopics(topics);
    this.startupPosition = requireValidStartupPosition(startupPosition, properties);

    this.deserializer = Objects.requireNonNull(deserializer);
    this.ingressIdentifier = Objects.requireNonNull(id);
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return Constants.KAFKA_INGRESS_TYPE;
  }

  public Properties properties() {
    return properties;
  }

  public List<String> topics() {
    return topics;
  }

  public KafkaIngressDeserializer<T> deserializer() {
    return deserializer;
  }

  public KafkaIngressStartupPosition startupPosition() {
    return startupPosition;
  }

  private static Properties requireValidProperties(Properties properties) {
    Objects.requireNonNull(properties);

    if (!properties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new IllegalArgumentException("Missing setting for Kafka address.");
    }

    // TODO: we eventually want to make the ingress work out-of-the-box without the need to set the
    // consumer group id
    if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new IllegalArgumentException("Missing setting for consumer group id.");
    }

    return properties;
  }

  private static List<String> requireValidTopics(List<String> topics) {
    Objects.requireNonNull(topics);

    if (topics.isEmpty()) {
      throw new IllegalArgumentException("Must define at least one Kafka topic to consume from.");
    }

    return topics;
  }

  private static KafkaIngressStartupPosition requireValidStartupPosition(
      KafkaIngressStartupPosition startupPosition, Properties properties) {
    if (startupPosition.isGroupOffsets()
        && !properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new IllegalStateException(
          "The ingress is configured to start from committed consumer group offsets in Kafka, but no consumer group id was set.\n"
              + "Please set the group id with the withConsumerGroupId(String) method.");
    }

    return startupPosition;
  }
}
