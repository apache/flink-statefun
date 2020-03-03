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

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.io.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A {@link KafkaEgressSerializer} used solely by sinks provided by the {@link
 * GenericKafkaSinkProvider}.
 *
 * <p>This serializer expects Protobuf messages of type {@link KafkaProducerRecord}, and simply
 * transforms those into Kafka's {@link ProducerRecord}.
 */
public final class GenericKafkaEgressSerializer implements KafkaEgressSerializer<Any> {

  private static final long serialVersionUID = 1L;

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Any any) {
    KafkaProducerRecord protobufProducerRecord = asKafkaProducerRecord(any);
    return toProducerRecord(protobufProducerRecord);
  }

  private static KafkaProducerRecord asKafkaProducerRecord(Any message) {
    if (!message.is(KafkaProducerRecord.class)) {
      throw new IllegalStateException(
          "The generic Kafka egress expects only messages of type "
              + KafkaProducerRecord.class.getName());
    }
    try {
      return message.unpack(KafkaProducerRecord.class);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KafkaProducerRecord.class.getName(), e);
    }
  }

  private static ProducerRecord<byte[], byte[]> toProducerRecord(
      KafkaProducerRecord protobufProducerRecord) {
    return new ProducerRecord<>(
        protobufProducerRecord.getTopic(),
        protobufProducerRecord.getKey().getBytes(StandardCharsets.UTF_8),
        protobufProducerRecord.getValueBytes().toByteArray());
  }
}
