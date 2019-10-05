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

package com.ververica.statefun.examples.ridesharing.simulator.simulation.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.statefun.examples.ridesharing.generated.InboundDriverMessage;
import com.ververica.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import com.ververica.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import com.ververica.statefun.examples.ridesharing.simulator.model.WebsocketDriverEvent;
import com.ververica.statefun.examples.ridesharing.simulator.model.WebsocketPassengerEvent;
import com.ververica.statefun.examples.ridesharing.simulator.services.KafkaDriverPublisher;
import com.ververica.statefun.examples.ridesharing.simulator.services.KafkaPassengerPublisher;
import com.ververica.statefun.examples.ridesharing.simulator.simulation.DriverMessaging;
import com.ververica.statefun.examples.ridesharing.simulator.simulation.PassengerMessaging;
import com.ververica.statefun.examples.ridesharing.simulator.simulation.engine.Scheduler;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Communication implements PassengerMessaging, DriverMessaging {
  private final KafkaPassengerPublisher passengerPublisher;
  private final KafkaDriverPublisher driverPublisher;
  private final SimpMessagingTemplate simpSender;
  private final String passengerWebSocketTopic;
  private final String driverWebSocketTopic;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Scheduler scheduler;

  @Autowired
  public Communication(
      KafkaPassengerPublisher passengerPublisher,
      KafkaDriverPublisher driverPublisher,
      SimpMessagingTemplate simpSender,
      @Value("${web-socket.topic.passenger}") String passengerWebSocketTopic,
      @Value("${web-socket.topic.driver}") String driverWebSocketTopic,
      Scheduler scheduler) {
    this.passengerPublisher = Objects.requireNonNull(passengerPublisher);
    this.driverPublisher = Objects.requireNonNull(driverPublisher);
    this.simpSender = Objects.requireNonNull(simpSender);
    this.passengerWebSocketTopic = Objects.requireNonNull(passengerWebSocketTopic);
    this.driverWebSocketTopic = Objects.requireNonNull(driverWebSocketTopic);
    this.scheduler = Objects.requireNonNull(scheduler);
  }

  public void incomingPassengerEvent(OutboundPassengerMessage message) {
    scheduler.enqueueTaskMessage(message.getPassengerId(), message);
  }

  public void incomingDriverEvent(OutboundDriverMessage message) {
    scheduler.enqueueTaskMessage(message.getDriverId(), message);
  }

  public void outgoingPassengerEvent(InboundPassengerMessage message) {
    passengerPublisher.accept(message);
  }

  public void outgoingDriverEvent(InboundDriverMessage message) {
    driverPublisher.accept(message);
  }

  public void broadcastPassengerSimulationEvent(WebsocketPassengerEvent passengerEvent) {
    String json = toJsonString(passengerEvent);
    simpSender.convertAndSend(passengerWebSocketTopic, json);
  }

  public void broadcastDriverSimulationEvent(WebsocketDriverEvent driverEvent) {
    if (driverEvent.getDriverStatus() == WebsocketDriverEvent.DriverStatus.IDLE) {
      // don't broadcast idle drivers, this is slightly too much.
      return;
    }
    String json = toJsonString(driverEvent);
    simpSender.convertAndSend(driverWebSocketTopic, json);
  }

  private String toJsonString(Object what) {
    try {
      return objectMapper.writeValueAsString(what);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
