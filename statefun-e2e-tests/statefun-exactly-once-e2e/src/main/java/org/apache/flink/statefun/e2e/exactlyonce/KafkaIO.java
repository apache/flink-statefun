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

package org.apache.flink.statefun.e2e.exactlyonce;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.e2e.exactlyonce.generated.ExactlyOnceVerification.InvokeCount;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaIO {

  static final String WRAPPED_MESSAGES_TOPIC_NAME = "wrapped-messages";
  static final String INVOKE_COUNTS_TOPIC_NAME = "invoke-counts";

  private final String kafkaAddress;

  KafkaIO(String kafkaAddress) {
    this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
  }

  EgressSpec<InvokeCount> getEgressSpec() {
    return KafkaEgressBuilder.forIdentifier(Constants.EGRESS_ID)
        .withKafkaAddress(kafkaAddress)
        .withExactlyOnceProducerSemantics(Duration.ofMinutes(1))
        .withSerializer(InvokeCountKafkaSerializer.class)
        .build();
  }

  private static final class InvokeCountKafkaSerializer
      implements KafkaEgressSerializer<InvokeCount> {

    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(InvokeCount invokeCount) {
      final byte[] key = invokeCount.getIdBytes().toByteArray();
      final byte[] value = invokeCount.toByteArray();

      return new ProducerRecord<>(INVOKE_COUNTS_TOPIC_NAME, key, value);
    }
  }
}
