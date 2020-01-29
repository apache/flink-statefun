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
package org.apache.flink.statefun.examples.ridesharing.simulator.services;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundDriverMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaDriverPublisher implements Consumer<InboundDriverMessage> {
  private final String topic;
  private KafkaTemplate<Object, Object> kafkaTemplate;

  @Autowired
  public KafkaDriverPublisher(
      KafkaTemplate<Object, Object> kafkaTemplateForJson,
      @Value("${kafka.topic.from-driver}") String topic) {
    this.kafkaTemplate = kafkaTemplateForJson;
    this.topic = topic;
  }

  @Override
  public void accept(InboundDriverMessage driver) {
    byte[] keyBytes = driver.getDriverId().getBytes(StandardCharsets.UTF_8);
    kafkaTemplate.send(topic, keyBytes, driver.toByteArray());
  }
}
