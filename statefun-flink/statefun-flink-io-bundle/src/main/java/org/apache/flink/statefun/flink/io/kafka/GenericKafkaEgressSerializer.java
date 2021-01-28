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

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.flink.common.types.TypedValueUtil;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A {@link KafkaEgressSerializer} used solely by sinks provided by the {@link
 * GenericKafkaSinkProvider}.
 *
 * <p>This serializer expects Protobuf messages of type {@link KafkaProducerRecord}, and simply
 * transforms those into Kafka's {@link ProducerRecord}.
 */
public final class GenericKafkaEgressSerializer implements KafkaEgressSerializer<TypedValue> {

  private static final long serialVersionUID = 1L;

  @Override
  public ProducerRecord<byte[], byte[]> serialize(TypedValue message) {
    KafkaProducerRecord protobufProducerRecord = asKafkaProducerRecord(message);
    return toProducerRecord(protobufProducerRecord);
  }

  private static KafkaProducerRecord asKafkaProducerRecord(TypedValue message) {
    if (!TypedValueUtil.isProtobufTypeOf(message, KafkaProducerRecord.getDescriptor())) {
      throw new IllegalStateException(
          "The generic Kafka egress expects only messages of type "
              + KafkaProducerRecord.class.getName());
    }
    try {
      return KafkaProducerRecord.parseFrom(message.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Unable to unpack message as a " + KafkaProducerRecord.class.getName(), e);
    }
  }

  private static ProducerRecord<byte[], byte[]> toProducerRecord(
      KafkaProducerRecord protobufProducerRecord) {
    final String key = protobufProducerRecord.getKey();
    final String topic = protobufProducerRecord.getTopic();
    final byte[] valueBytes = protobufProducerRecord.getValueBytes().toByteArray();

    if (key == null || key.isEmpty()) {
      return new ProducerRecord<>(topic, valueBytes);
    } else {
      return new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), valueBytes);
    }
  }
}
