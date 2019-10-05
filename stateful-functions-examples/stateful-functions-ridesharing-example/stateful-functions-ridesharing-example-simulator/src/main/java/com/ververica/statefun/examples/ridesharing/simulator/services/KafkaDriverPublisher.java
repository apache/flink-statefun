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

package com.ververica.statefun.examples.ridesharing.simulator.services;

import com.ververica.statefun.examples.ridesharing.generated.InboundDriverMessage;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
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
    ListenableFuture<SendResult<Object, Object>> future =
        kafkaTemplate.send(topic, keyBytes, driver.toByteArray());

    future.addCallback(
        new ListenableFutureCallback<SendResult<Object, Object>>() {
          @Override
          public void onFailure(Throwable throwable) {
            log.warn("Failed sending an event to kafka", throwable);
          }

          @Override
          public void onSuccess(SendResult<Object, Object> objectObjectSendResult) {}
        });
  }
}
