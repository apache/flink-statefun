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

package com.ververica.statefun.flink.io.kafka;

import com.ververica.statefun.sdk.kafka.KafkaEgressSerializer;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KafkaSerializationSchemaDelegate<T> implements KafkaSerializationSchema<T> {

  private static final long serialVersionUID = 1L;

  private final KafkaEgressSerializer<T> serializer;

  KafkaSerializationSchemaDelegate(KafkaEgressSerializer<T> serializer) {
    this.serializer = Objects.requireNonNull(serializer);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long aLong) {
    return serializer.serialize(t);
  }
}
