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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from Apache Kafka.
 *
 * @param <T> The type consumed from Kafka.
 */
public final class KafkaIngressBuilder<T> {

  private final IngressIdentifier<T> id;
  private final List<String> topics = new ArrayList<>();
  private final Properties properties = new Properties();
  private String consumerGroupId;
  private Class<? extends KafkaIngressDeserializer<T>> deserializerClass;
  private String kafkaAddress;

  private KafkaIngressBuilder(IngressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param id A unique ingress identifier.
   * @param <T> The type consumed from Kafka.
   * @return A new {@link KafkaIngressBuilder}.
   */
  public static <T> KafkaIngressBuilder<T> forIdentifier(IngressIdentifier<T> id) {
    return new KafkaIngressBuilder<>(id);
  }

  /** @param consumerGroupId the consumer group id to use. */
  public KafkaIngressBuilder<T> withConsumerGroupId(String consumerGroupId) {
    this.consumerGroupId = Objects.requireNonNull(consumerGroupId);
    return this;
  }

  /** @param kafkaAddress Comma separated addresses of the brokers. */
  public KafkaIngressBuilder<T> withKafkaAddress(String kafkaAddress) {
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
    return this;
  }

  /** @param topic The name of the topic that should be consumed. */
  public KafkaIngressBuilder<T> withTopic(String topic) {
    topics.add(topic);
    return this;
  }

  /** @param topics A list of topics that should be consumed. */
  public KafkaIngressBuilder<T> addTopics(List<String> topics) {
    this.topics.addAll(topics);
    return this;
  }

  /** A configuration property for the KafkaConsumer. */
  public KafkaIngressBuilder<T> withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  /** A configuration property for the KafkaProducer. */
  public KafkaIngressBuilder<T> withProperty(String name, String value) {
    this.properties.put(name, value);
    return this;
  }

  /**
   * @param deserializerClass The deserializer used to convert between Kafka's byte messages and
   *     java objects.
   */
  public KafkaIngressBuilder<T> withDeserializer(
      Class<? extends KafkaIngressDeserializer<T>> deserializerClass) {
    this.deserializerClass = Objects.requireNonNull(deserializerClass);
    return this;
  }

  /** @return A new {@link IngressSpec}. */
  public IngressSpec<T> build() {
    return new KafkaIngressSpec<>(
        id, kafkaAddress, properties, topics, deserializerClass, consumerGroupId);
  }
}
