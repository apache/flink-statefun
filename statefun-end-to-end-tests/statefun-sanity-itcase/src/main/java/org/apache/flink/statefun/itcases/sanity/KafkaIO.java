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

package org.apache.flink.statefun.itcases.sanity;

import java.util.Objects;
import org.apache.flink.statefun.itcases.sanity.generated.VerificationMessages.Command;
import org.apache.flink.statefun.itcases.sanity.generated.VerificationMessages.StateSnapshot;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaIO {

  static final String COMMAND_TOPIC_NAME = "commands";
  static final String STATE_SNAPSHOTS_TOPIC_NAME = "state-snapshots";

  private final String kafkaAddress;

  KafkaIO(String kafkaAddress) {
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
  }

  IngressSpec<Command> getIngressSpec() {
    return KafkaIngressBuilder.forIdentifier(Constants.COMMAND_INGRESS_ID)
        .withKafkaAddress(kafkaAddress)
        .withConsumerGroupId("sanity-itcase")
        .withTopic(COMMAND_TOPIC_NAME)
        .withDeserializer(CommandKafkaDeserializer.class)
        .withStartupPosition(KafkaIngressStartupPosition.fromEarliest())
        .build();
  }

  EgressSpec<StateSnapshot> getEgressSpec() {
    return KafkaEgressBuilder.forIdentifier(Constants.STATE_SNAPSHOT_EGRESS_ID)
        .withKafkaAddress(kafkaAddress)
        .withSerializer(StateSnapshotKafkaSerializer.class)
        .build();
  }

  private static final class CommandKafkaDeserializer implements KafkaIngressDeserializer<Command> {

    private static final long serialVersionUID = 1L;

    @Override
    public Command deserialize(ConsumerRecord<byte[], byte[]> input) {
      try {
        return Command.parseFrom(input.value());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class StateSnapshotKafkaSerializer
      implements KafkaEgressSerializer<StateSnapshot> {

    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(StateSnapshot stateSnapshot) {
      final byte[] key = stateSnapshot.getFrom().toByteArray();
      final byte[] value = stateSnapshot.toByteArray();

      return new ProducerRecord<>(STATE_SNAPSHOTS_TOPIC_NAME, key, value);
    }
  }
}
