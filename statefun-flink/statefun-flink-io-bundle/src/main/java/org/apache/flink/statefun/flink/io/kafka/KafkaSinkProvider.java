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
package org.apache.flink.statefun.flink.io.kafka;

import static org.apache.flink.util.StringUtils.generateRandomAlphanumericString;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaProducerSemantic;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkProvider implements SinkProvider {

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> egressSpec) {
    KafkaEgressSpec<T> spec = asSpec(egressSpec);

    Properties properties = new Properties();
    properties.putAll(spec.properties());
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.kafkaAddress());

    Semantic producerSemantic = semanticFromSpec(spec);
    if (producerSemantic == Semantic.EXACTLY_ONCE) {
      properties.setProperty(
          ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
          String.valueOf(spec.semantic().asExactlyOnceSemantic().transactionTimeout().toMillis()));
    }

    return new FlinkKafkaProducer<>(
        randomKafkaTopic(),
        serializerFromSpec(spec),
        properties,
        producerSemantic,
        spec.kafkaProducerPoolSize());
  }

  private <T> KafkaSerializationSchema<T> serializerFromSpec(KafkaEgressSpec<T> spec) {
    KafkaEgressSerializer<T> serializer = ReflectionUtil.instantiate(spec.serializerClass());
    return new KafkaSerializationSchemaDelegate<>(serializer);
  }

  private static <T> Semantic semanticFromSpec(KafkaEgressSpec<T> spec) {
    final KafkaProducerSemantic semantic = spec.semantic();
    if (semantic.isExactlyOnceSemantic()) {
      return Semantic.EXACTLY_ONCE;
    } else if (semantic.isAtLeastOnceSemantic()) {
      return Semantic.AT_LEAST_ONCE;
    } else if (semantic.isNoSemantic()) {
      return Semantic.NONE;
    } else {
      throw new IllegalArgumentException("Unknown producer semantic " + semantic.getClass());
    }
  }

  private static <T> KafkaEgressSpec<T> asSpec(EgressSpec<T> spec) {
    if (spec instanceof KafkaEgressSpec) {
      return (KafkaEgressSpec<T>) spec;
    }
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }

  private static String randomKafkaTopic() {
    return "__stateful_functions_random_topic_"
        + generateRandomAlphanumericString(ThreadLocalRandom.current(), 16);
  }
}
