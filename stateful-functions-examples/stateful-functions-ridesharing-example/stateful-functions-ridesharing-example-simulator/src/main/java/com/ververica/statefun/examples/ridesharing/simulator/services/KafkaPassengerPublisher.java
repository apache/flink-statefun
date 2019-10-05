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

import com.ververica.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
public class KafkaPassengerPublisher implements Consumer<InboundPassengerMessage> {
  private final String topic;
  private final KafkaTemplate<Object, Object> kafkaTemplate;

  @Autowired
  public KafkaPassengerPublisher(
      KafkaTemplate<Object, Object> kafkaTemplate,
      @Value("${kafka.topic.from-passenger}") String topic) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate);
    this.topic = Objects.requireNonNull(topic);
  }

  @Override
  public void accept(InboundPassengerMessage passenger) {
    byte[] bytes = passenger.getPassengerId().getBytes(StandardCharsets.UTF_8);
    kafkaTemplate
        .send(topic, bytes, passenger.toByteArray())
        .addCallback(
            new ListenableFutureCallback<SendResult<Object, Object>>() {
              @Override
              public void onFailure(@NonNull Throwable throwable) {
                log.warn("couldn't send passenger data.", throwable);
              }

              @Override
              public void onSuccess(SendResult<Object, Object> objectObjectSendResult) {
                log.info("Sent passenger data");
              }
            });
  }
}
