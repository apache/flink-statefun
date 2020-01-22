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
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;

public final class KafkaEgressSpec<OutT> implements EgressSpec<OutT> {
  private final Class<? extends KafkaEgressSerializer<OutT>> serializer;
  private final String kafkaAddress;
  private final Properties properties;
  private final EgressIdentifier<OutT> id;
  private final int kafkaProducerPoolSize;
  private final KafkaProducerSemantic semantic;
  private final Duration transactionTimeoutDuration;

  KafkaEgressSpec(
      EgressIdentifier<OutT> id,
      Class<? extends KafkaEgressSerializer<OutT>> serializer,
      String kafkaAddress,
      Properties properties,
      int kafkaProducerPoolSize,
      KafkaProducerSemantic semantic,
      Duration transactionTimeoutDuration) {
    this.serializer = Objects.requireNonNull(serializer);
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
    this.properties = Objects.requireNonNull(properties);
    this.id = Objects.requireNonNull(id);
    this.kafkaProducerPoolSize = kafkaProducerPoolSize;
    this.semantic = Objects.requireNonNull(semantic);
    this.transactionTimeoutDuration = Objects.requireNonNull(transactionTimeoutDuration);
  }

  @Override
  public EgressIdentifier<OutT> id() {
    return id;
  }

  @Override
  public EgressType type() {
    return Constants.KAFKA_EGRESS_TYPE;
  }

  public Class<? extends KafkaEgressSerializer<OutT>> serializerClass() {
    return serializer;
  }

  public String kafkaAddress() {
    return kafkaAddress;
  }

  public Properties properties() {
    return properties;
  }

  public int kafkaProducerPoolSize() {
    return kafkaProducerPoolSize;
  }

  public KafkaProducerSemantic semantic() {
    return semantic;
  }

  public Duration transactionTimeoutDuration() {
    return transactionTimeoutDuration;
  }
}
