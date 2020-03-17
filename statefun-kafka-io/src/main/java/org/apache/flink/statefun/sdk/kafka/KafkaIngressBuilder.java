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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from Apache Kafka.
 *
 * @param <T> The type consumed from Kafka.
 */
public final class KafkaIngressBuilder<T> {

  private final IngressIdentifier<T> id;
  private final List<String> topics = new ArrayList<>();
  private final Properties properties = new Properties();

  private OptionalConfig<String> consumerGroupId = OptionalConfig.withoutDefault();
  private OptionalConfig<KafkaIngressDeserializer<T>> deserializer =
      OptionalConfig.withoutDefault();
  private OptionalConfig<String> kafkaAddress = OptionalConfig.withoutDefault();
  private OptionalConfig<KafkaIngressAutoResetPosition> autoResetPosition =
      OptionalConfig.withDefault(KafkaIngressAutoResetPosition.LATEST);
  private OptionalConfig<KafkaIngressStartupPosition> startupPosition =
      OptionalConfig.withDefault(KafkaIngressStartupPosition.fromLatest());

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
    this.consumerGroupId.set(consumerGroupId);
    return this;
  }

  /** @param kafkaAddress Comma separated addresses of the brokers. */
  public KafkaIngressBuilder<T> withKafkaAddress(String kafkaAddress) {
    this.kafkaAddress.set(kafkaAddress);
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
    Objects.requireNonNull(name);
    Objects.requireNonNull(value);
    this.properties.setProperty(name, value);
    return this;
  }

  /**
   * @param deserializerClass The deserializer used to convert between Kafka's byte messages and
   *     java objects.
   */
  public KafkaIngressBuilder<T> withDeserializer(
      Class<? extends KafkaIngressDeserializer<T>> deserializerClass) {
    Objects.requireNonNull(deserializerClass);
    this.deserializer.set(instantiateDeserializer(deserializerClass));
    return this;
  }

  /**
   * @param autoResetPosition the auto offset reset position to use, in case consumed offsets are
   *     invalid.
   */
  public KafkaIngressBuilder<T> withAutoResetPosition(
      KafkaIngressAutoResetPosition autoResetPosition) {
    this.autoResetPosition.set(autoResetPosition);
    return this;
  }

  /**
   * Configures the position that the ingress should start consuming from. By default, the startup
   * position is {@link KafkaIngressStartupPosition#fromLatest()}.
   *
   * <p>Note that this configuration only affects the position when starting the application from a
   * fresh start. When restoring the application from a savepoint, the ingress will always start
   * consuming from the offsets persisted in the savepoint.
   *
   * @param startupPosition the position that the Kafka ingress should start consuming from.
   * @see KafkaIngressStartupPosition
   */
  public KafkaIngressBuilder<T> withStartupPosition(KafkaIngressStartupPosition startupPosition) {
    this.startupPosition.set(startupPosition);
    return this;
  }

  /** @return A new {@link KafkaIngressSpec}. */
  public KafkaIngressSpec<T> build() {
    Properties properties = resolveKafkaProperties();
    return new KafkaIngressSpec<>(
        id, properties, topics, deserializer.get(), startupPosition.get());
  }

  private Properties resolveKafkaProperties() {
    Properties resultProps = new Properties();
    resultProps.putAll(properties);

    // for all configuration passed using named methods, overwrite corresponding properties
    kafkaAddress.overwritePropertiesIfPresent(resultProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    autoResetPosition.overwritePropertiesIfPresent(
        resultProps, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    consumerGroupId.overwritePropertiesIfPresent(resultProps, ConsumerConfig.GROUP_ID_CONFIG);

    return resultProps;
  }

  private static <T extends KafkaIngressDeserializer<?>> T instantiateDeserializer(
      Class<T> deserializerClass) {
    try {
      Constructor<T> defaultConstructor = deserializerClass.getDeclaredConstructor();
      defaultConstructor.setAccessible(true);
      return defaultConstructor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer "
              + deserializerClass.getName()
              + "; has no default constructor",
          e);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer " + deserializerClass.getName(), e);
    }
  }

  // ========================================================================================
  //  Methods for runtime usage
  // ========================================================================================

  @ForRuntime
  KafkaIngressBuilder<T> withDeserializer(KafkaIngressDeserializer<T> deserializer) {
    this.deserializer.set(deserializer);
    return this;
  }
}
