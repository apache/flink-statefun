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

package org.apache.flink.statefun.e2e.routablekafka;

import java.util.Objects;
import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.MessageWithAddress;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaIO {

  static final String TAGGED_MESSAGES_TOPIC_NAME = "tagged-messages";

  private final String kafkaAddress;

  KafkaIO(String kafkaAddress) {
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
  }

  EgressSpec<MessageWithAddress> getEgressSpec() {
    return KafkaEgressBuilder.forIdentifier(Constants.EGRESS_ID)
        .withKafkaAddress(kafkaAddress)
        .withSerializer(TaggedMessageKafkaSerializer.class)
        .build();
  }

  private static final class TaggedMessageKafkaSerializer
      implements KafkaEgressSerializer<MessageWithAddress> {

    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(MessageWithAddress taggedMessages) {
      final byte[] key = taggedMessages.getFrom().getIdBytes().toByteArray();
      final byte[] value = taggedMessages.toByteArray();

      return new ProducerRecord<>(TAGGED_MESSAGES_TOPIC_NAME, key, value);
    }
  }
}
