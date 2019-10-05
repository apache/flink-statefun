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

package com.ververica.statefun.sdk.kafka;

import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The deserialization schema describes how to turn the Kafka ConsumerRecords into data types that
 * are processed by the system.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface KafkaIngressDeserializer<T> extends Serializable {

  /**
   * Deserializes the Kafka record.
   *
   * @param input Kafka record to be deserialized.
   * @return The deserialized message as an object (null if the message cannot be deserialized).
   */
  T deserialize(ConsumerRecord<byte[], byte[]> input);
}
