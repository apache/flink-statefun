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

import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ProtobufKafkaIngressDeserializerTest {

  @Test
  public void exampleUsage() {
    ProtobufKafkaIngressDeserializer deserializer =
        new ProtobufKafkaIngressDeserializer(
            "classpath:test-descriptors.bin", "com.ververica.test.SimpleMessage");

    // the actual message doesn't matter, since we only want to show that the parser is initialized.
    byte[] messageBytes = new byte[0];

    Message message =
        deserializer.deserialize(new ConsumerRecord<>("", 0, 0, "".getBytes(), messageBytes));

    assertThat(message, notNullValue());
  }
}
