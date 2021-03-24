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

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class RoutableProtobufKafkaIngressDeserializer
    implements KafkaIngressDeserializer<Message> {

  private static final long serialVersionUID = 1L;

  private final Map<String, RoutingConfig> routingConfigs;

  RoutableProtobufKafkaIngressDeserializer(Map<String, RoutingConfig> routingConfigs) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kafka ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
  }

  @Override
  public Message deserialize(ConsumerRecord<byte[], byte[]> input) {
    final String topic = input.topic();
    final byte[] payload = input.value();
    final byte[] key = requireNonNullKey(input.key());
    final String id = new String(key, StandardCharsets.UTF_8);

    final RoutingConfig routingConfig = routingConfigs.get(topic);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from topic [" + topic + "], but no routing config was specified.");
    }
    return AutoRoutable.newBuilder()
        .setConfig(routingConfig)
        .setId(id)
        .setPayloadBytes(MoreByteStrings.wrap(payload))
        .build();
  }

  private byte[] requireNonNullKey(byte[] key) {
    if (key == null) {
      IngressType tpe = ProtobufKafkaIngressTypes.ROUTABLE_PROTOBUF_KAFKA_INGRESS_TYPE;
      throw new IllegalStateException(
          "The "
              + tpe.namespace()
              + "/"
              + tpe.type()
              + " ingress requires a UTF-8 key set for each record.");
    }
    return key;
  }
}
