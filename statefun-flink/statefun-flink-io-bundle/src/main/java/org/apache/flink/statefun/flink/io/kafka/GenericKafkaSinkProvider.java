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

import static org.apache.flink.statefun.flink.io.kafka.KafkaEgressSpecJsonParser.exactlyOnceDeliveryTxnTimeout;
import static org.apache.flink.statefun.flink.io.kafka.KafkaEgressSpecJsonParser.kafkaAddress;
import static org.apache.flink.statefun.flink.io.kafka.KafkaEgressSpecJsonParser.kafkaClientProperties;
import static org.apache.flink.statefun.flink.io.kafka.KafkaEgressSpecJsonParser.optionalDeliverySemantic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

final class GenericKafkaSinkProvider implements SinkProvider {

  private final KafkaSinkProvider delegateProvider = new KafkaSinkProvider();

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
    KafkaEgressSpec<T> kafkaEgressSpec = asKafkaEgressSpec(spec);
    return delegateProvider.forSpec(kafkaEgressSpec);
  }

  private static <T> KafkaEgressSpec<T> asKafkaEgressSpec(EgressSpec<T> spec) {
    if (!(spec instanceof JsonEgressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonEgressSpec<T> casted = (JsonEgressSpec<T>) spec;

    EgressIdentifier<T> id = casted.id();
    validateConsumedType(id);

    JsonNode json = casted.json();

    KafkaEgressBuilder<T> kafkaEgressBuilder = KafkaEgressBuilder.forIdentifier(id);
    kafkaEgressBuilder
        .withKafkaAddress(kafkaAddress(json))
        .withProperties(kafkaClientProperties(json))
        .withSerializer(serializerClass());

    optionalDeliverySemantic(json)
        .ifPresent(
            semantic -> {
              switch (semantic) {
                case AT_LEAST_ONCE:
                  kafkaEgressBuilder.withAtLeastOnceProducerSemantics();
                  break;
                case EXACTLY_ONCE:
                  kafkaEgressBuilder.withExactlyOnceProducerSemantics(
                      exactlyOnceDeliveryTxnTimeout(json));
                  break;
                case NONE:
                  kafkaEgressBuilder.withNoProducerSemantics();
                  break;
                default:
                  throw new IllegalStateException("Unrecognized producer semantic: " + semantic);
              }
            });

    return kafkaEgressBuilder.build();
  }

  private static void validateConsumedType(EgressIdentifier<?> id) {
    Class<?> consumedType = id.consumedType();
    if (TypedValue.class != consumedType) {
      throw new IllegalArgumentException(
          "Generic Kafka egress is only able to consume messages types of "
              + TypedValue.class.getName()
              + " but "
              + consumedType.getName()
              + " is provided.");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> serializerClass() {
    // this cast is safe, because we've already validated that the consumed type is Any.
    return (Class<T>) GenericKafkaEgressSerializer.class;
  }
}
