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

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

/**
 * A builder class for creating an {@link EgressSpec} that writes data out to a Kafka cluster. By
 * default the egress will use {@link #withAtLeastOnceProducerSemantics()}.
 *
 * @param <OutT> The type written out to the cluster by the Egress.
 */
public final class KafkaEgressBuilder<OutT> {
  private final EgressIdentifier<OutT> id;
  private Class<? extends KafkaEgressSerializer<OutT>> serializer;
  private String kafkaAddress;
  private Properties properties = new Properties();
  private int kafkaProducerPoolSize = 5;
  private KafkaProducerSemantic semantic = KafkaProducerSemantic.AT_LEAST_ONCE;
  private Duration transactionTimeoutDuration = Duration.ZERO;

  private KafkaEgressBuilder(EgressIdentifier<OutT> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param egressIdentifier A unique egress identifier.
   * @param <OutT> The type the egress will output.
   * @return A {@link KafkaIngressBuilder}.
   */
  public static <OutT> KafkaEgressBuilder<OutT> forIdentifier(
      EgressIdentifier<OutT> egressIdentifier) {
    return new KafkaEgressBuilder<>(egressIdentifier);
  }

  /** @param kafkaAddress Comma separated addresses of the brokers. */
  public KafkaEgressBuilder<OutT> withKafkaAddress(String kafkaAddress) {
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
    return this;
  }

  /** A configuration property for the KafkaProducer. */
  public KafkaEgressBuilder<OutT> withProperty(String key, Object value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    properties.put(key, value);
    return this;
  }

  /** Configuration properties for the KafkaProducer. */
  public KafkaEgressBuilder<OutT> withProperties(Properties properties) {
    Objects.requireNonNull(properties);
    this.properties.putAll(properties);
    return this;
  }

  /**
   * @param serializer A serializer schema for turning user objects into a kafka-consumable byte[]
   *     supporting key/value messages.
   */
  public KafkaEgressBuilder<OutT> withSerializer(
      Class<? extends KafkaEgressSerializer<OutT>> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
    return this;
  }

  /** @param poolSize Overwrite default KafkaProducers pool size. The default is 5. */
  public KafkaEgressBuilder<OutT> withKafkaProducerPoolSize(int poolSize) {
    this.kafkaProducerPoolSize = poolSize;
    return this;
  }

  /**
   * KafkaProducerSemantic.EXACTLY_ONCE the egress will write all messages in a Kafka transaction
   * that will be committed to Kafka on a checkpoint.
   *
   * <p>With exactly-once producer semantics, users must also specify the transaction timeout. Note
   * that this value must not be larger than the {@code transaction.max.timeout.ms} value configured
   * on Kafka brokers (by default, this is 15 minutes).
   *
   * @param transactionTimeoutDuration the transaction timeout.
   */
  public KafkaEgressBuilder<OutT> withExactlyOnceProducerSemantics(
      Duration transactionTimeoutDuration) {
    Objects.requireNonNull(
        transactionTimeoutDuration, "a transaction timeout duration must be provided.");
    if (transactionTimeoutDuration == Duration.ZERO) {
      throw new IllegalArgumentException(
          "Transaction timeout durations must be larger than 0 when using exactly-once producer semantics.");
    }

    this.semantic = KafkaProducerSemantic.EXACTLY_ONCE;
    this.transactionTimeoutDuration = transactionTimeoutDuration;
    return this;
  }

  /**
   * KafkaProducerSemantic.AT_LEAST_ONCE the egress will wait for all outstanding messages in the
   * Kafka buffers to be acknowledged by the Kafka producer on a checkpoint.
   */
  public KafkaEgressBuilder<OutT> withAtLeastOnceProducerSemantics() {
    this.semantic = KafkaProducerSemantic.AT_LEAST_ONCE;
    return this;
  }

  /**
   * KafkaProducerSemantic.NONE means that nothing will be guaranteed. Messages can be lost and/or
   * duplicated in case of failure.
   */
  public KafkaEgressBuilder<OutT> withNoProducerSemantics() {
    this.semantic = KafkaProducerSemantic.NONE;
    return this;
  }

  /** @return An {@link EgressSpec} that can be used in a {@code StatefulFunctionModule}. */
  public EgressSpec<OutT> build() {
    return new KafkaEgressSpec<>(
        id,
        serializer,
        kafkaAddress,
        properties,
        kafkaProducerPoolSize,
        semantic,
        transactionTimeoutDuration);
  }
}
