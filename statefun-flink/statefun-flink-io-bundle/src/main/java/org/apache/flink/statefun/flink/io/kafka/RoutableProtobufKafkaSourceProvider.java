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

import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.kafkaAddress;
import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.kafkaClientProperties;
import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.optionalAutoOffsetResetPosition;
import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.optionalConsumerGroupId;
import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.optionalStartupPosition;
import static org.apache.flink.statefun.flink.io.kafka.KafkaIngressSpecJsonParser.routableTopics;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class RoutableProtobufKafkaSourceProvider implements SourceProvider {

  private final KafkaSourceProvider delegateProvider = new KafkaSourceProvider();

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    KafkaIngressSpec<T> kafkaIngressSpec = asKafkaIngressSpec(spec);
    return delegateProvider.forSpec(kafkaIngressSpec);
  }

  private static <T> KafkaIngressSpec<T> asKafkaIngressSpec(IngressSpec<T> spec) {
    if (!(spec instanceof JsonIngressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonIngressSpec<T> casted = (JsonIngressSpec<T>) spec;

    IngressIdentifier<T> id = casted.id();
    Class<T> producedType = casted.id().producedType();
    if (!Message.class.isAssignableFrom(producedType)) {
      throw new IllegalArgumentException(
          "ProtocolBuffer based ingress is only able to produce types that derive from "
              + Message.class.getName()
              + " but "
              + producedType.getName()
              + " is provided.");
    }

    JsonNode json = casted.json();

    Map<String, RoutingConfig> routableTopics = routableTopics(json);

    KafkaIngressBuilder<T> kafkaIngressBuilder = KafkaIngressBuilder.forIdentifier(id);
    kafkaIngressBuilder
        .withKafkaAddress(kafkaAddress(json))
        .withProperties(kafkaClientProperties(json))
        .addTopics(new ArrayList<>(routableTopics.keySet()));

    optionalConsumerGroupId(json).ifPresent(kafkaIngressBuilder::withConsumerGroupId);
    optionalAutoOffsetResetPosition(json).ifPresent(kafkaIngressBuilder::withAutoResetPosition);
    optionalStartupPosition(json).ifPresent(kafkaIngressBuilder::withStartupPosition);

    KafkaIngressBuilderApiExtension.withDeserializer(
        kafkaIngressBuilder, deserializer(routableTopics));

    return kafkaIngressBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T> KafkaIngressDeserializer<T> deserializer(
      Map<String, RoutingConfig> routingConfig) {
    // this cast is safe since we've already checked that T is a Message
    return (KafkaIngressDeserializer<T>)
        new RoutableProtobufKafkaIngressDeserializer(routingConfig);
  }
}
