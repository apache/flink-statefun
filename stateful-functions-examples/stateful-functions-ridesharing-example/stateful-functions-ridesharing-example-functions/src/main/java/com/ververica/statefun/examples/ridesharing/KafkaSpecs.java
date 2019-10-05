/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.examples.ridesharing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ververica.statefun.examples.ridesharing.generated.InboundDriverMessage;
import com.ververica.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.kafka.KafkaEgressBuilder;
import com.ververica.statefun.sdk.kafka.KafkaEgressSerializer;
import com.ververica.statefun.sdk.kafka.KafkaIngressBuilder;
import com.ververica.statefun.sdk.kafka.KafkaIngressDeserializer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaSpecs {

  private static final String KAFKA_SERVER = "kafka-broker:9092";
  private static final String TO_PASSENGER_KAFKA_TOPIC_NAME = "to-passenger";
  private static final String TO_DRIVER_TOPIC_NAME = "to-driver";
  private static final String FROM_DRIVER_TOPIC_NAME = "from-driver";
  private static final String FROM_PASSENGER_TOPIC_NAME = "from-passenger";

  static IngressSpec<InboundDriverMessage> FROM_DRIVER_SPEC =
      KafkaIngressBuilder.forIdentifier(Identifiers.FROM_DRIVER)
          .withKafkaAddress(KAFKA_SERVER)
          .withTopic(FROM_DRIVER_TOPIC_NAME)
          .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "statefun-from-driver-group")
          .withDeserializer(FromDriverDeserializer.class)
          .build();

  static IngressSpec<InboundPassengerMessage> FROM_PASSENGER_SPEC =
      KafkaIngressBuilder.forIdentifier(Identifiers.FROM_PASSENGERS)
          .withKafkaAddress(KAFKA_SERVER)
          .withTopic(FROM_PASSENGER_TOPIC_NAME)
          .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "statefun-from-passenger-group")
          .withDeserializer(FromPassengersDeserializer.class)
          .build();

  static EgressSpec<OutboundPassengerMessage> TO_PASSENGER_SPEC =
      KafkaEgressBuilder.forIdentifier(Identifiers.TO_PASSENGER_EGRESS)
          .withKafkaAddress(KAFKA_SERVER)
          .withSerializer(ToPassengersSerializer.class)
          .build();

  static EgressSpec<OutboundDriverMessage> TO_DRIVER_SPEC =
      KafkaEgressBuilder.forIdentifier(Identifiers.TO_OUTBOUND_DRIVER)
          .withKafkaAddress(KAFKA_SERVER)
          .withSerializer(ToDriverSerializer.class)
          .build();

  private static final class FromDriverDeserializer
      implements KafkaIngressDeserializer<InboundDriverMessage> {

    private static final long serialVersionUID = 1;

    @Override
    public InboundDriverMessage deserialize(ConsumerRecord<byte[], byte[]> input) {
      try {
        return InboundDriverMessage.parseFrom(input.value());
      } catch (InvalidProtocolBufferException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private static final class FromPassengersDeserializer
      implements KafkaIngressDeserializer<InboundPassengerMessage> {

    private static final long serialVersionUID = 1;

    @Override
    public InboundPassengerMessage deserialize(ConsumerRecord<byte[], byte[]> input) {
      try {
        return InboundPassengerMessage.parseFrom(input.value());
      } catch (InvalidProtocolBufferException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private static final class ToPassengersSerializer
      implements KafkaEgressSerializer<OutboundPassengerMessage> {

    private static final long serialVersionUID = 1;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutboundPassengerMessage message) {
      byte[] key = message.getPassengerId().getBytes(StandardCharsets.UTF_8);
      byte[] value = message.toByteArray();
      return new ProducerRecord<>(TO_PASSENGER_KAFKA_TOPIC_NAME, key, value);
    }
  }

  private static final class ToDriverSerializer
      implements KafkaEgressSerializer<OutboundDriverMessage> {

    private static final long serialVersionUID = 1;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutboundDriverMessage message) {
      byte[] key = message.getDriverId().getBytes(StandardCharsets.UTF_8);
      byte[] value = message.toByteArray();
      return new ProducerRecord<>(TO_DRIVER_TOPIC_NAME, key, value);
    }
  }
}
