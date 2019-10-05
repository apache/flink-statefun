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

import com.google.protobuf.InvalidProtocolBufferException;
import com.ververica.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.simulator.simulation.messaging.Communication;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {
  private final Communication simulation;

  @Autowired
  public KafkaConsumerService(Communication simulation) {
    this.simulation = Objects.requireNonNull(simulation);
  }

  @KafkaListener(topics = "${kafka.topic.to-passenger}", groupId = "passengers")
  public void toPassenger(@Payload byte[] message) throws InvalidProtocolBufferException {
    OutboundPassengerMessage passengerMessage = OutboundPassengerMessage.parseFrom(message);
    simulation.incomingPassengerEvent(passengerMessage);
  }

  @KafkaListener(topics = "${kafka.topic.to-driver}", groupId = "drivers")
  public void toDriver(@Payload byte[] message) throws InvalidProtocolBufferException {
    OutboundDriverMessage driverMessage = OutboundDriverMessage.parseFrom(message);
    simulation.incomingDriverEvent(driverMessage);
  }
}
