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
package org.apache.flink.statefun.examples.ridesharing.simulator.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import org.apache.flink.statefun.examples.ridesharing.simulator.model.WebsocketPassengerEvent;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.LifecycleMessages;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.Simulatee;

@Slf4j
public class Passenger implements Simulatee {
  private final PassengerMessaging simulation;
  private final StateMachine<WebsocketPassengerEvent.PassengerStatus> stateMachine;
  private final String id;
  private final int startCell;
  private final int endCell;

  private String driverId;

  Passenger(PassengerMessaging simulation, String id, int startCell, int endCell) {
    this.simulation = simulation;
    this.id = id;
    this.startCell = startCell;
    this.endCell = endCell;
    this.stateMachine = passengerStateMachine();
  }

  /**
   * when this state machine initializes we send a ride request, and move to the next state ({@link
   * WebsocketPassengerEvent.PassengerStatus#REQUESTING}
   */
  @SuppressWarnings("unused")
  private WebsocketPassengerEvent.PassengerStatus init(LifecycleMessages.Initialization ignored) {

    final InboundPassengerMessage rideRequest =
        InboundPassengerMessage.newBuilder()
            .setPassengerId(id)
            .setRequestRide(
                InboundPassengerMessage.RequestRide.newBuilder()
                    .setStartGeoCell(startCell)
                    .setEndGeoCell(endCell))
            .build();

    // send to application
    simulation.outgoingPassengerEvent(rideRequest);

    // send to the web socket
    simulation.broadcastPassengerSimulationEvent(
        WebsocketPassengerEvent.builder()
            .passengerId(id)
            .startLocation(startCell)
            .endLocation(endCell)
            .status(WebsocketPassengerEvent.PassengerStatus.REQUESTING)
            .rideId("") // we don't have it yet.
            .build());

    // next state would be requesting a ride
    return WebsocketPassengerEvent.PassengerStatus.REQUESTING;
  }

  /** The ride failed, couldn't find a driver nearby. */
  private WebsocketPassengerEvent.PassengerStatus rideFailed(OutboundPassengerMessage message) {
    final String failedRideId = message.getRideFailed().getRideId();
    //
    // notify the websocket
    //
    simulation.broadcastPassengerSimulationEvent(
        WebsocketPassengerEvent.builder()
            .passengerId(id)
            .startLocation(startCell)
            .endLocation(endCell)
            .status(WebsocketPassengerEvent.PassengerStatus.FAIL)
            .rideId(failedRideId)
            .build());

    return WebsocketPassengerEvent.PassengerStatus.FAIL;
  }

  /** A driver was found, now waiting for the pickup to happen. */
  private WebsocketPassengerEvent.PassengerStatus driverFound(OutboundPassengerMessage message) {
    OutboundPassengerMessage.DriverHasBeenFound driverFound = message.getDriverFound();

    simulation.broadcastPassengerSimulationEvent(
        WebsocketPassengerEvent.builder()
            .rideId("") // TODO: ?
            .passengerId(id)
            .startLocation(startCell)
            .endLocation(endCell)
            .driverId(driverFound.getDriverId())
            .status(WebsocketPassengerEvent.PassengerStatus.WAITING_FOR_RIDE_TO_START)
            .build());

    // capture the driver id
    driverId = driverFound.getDriverId();

    return WebsocketPassengerEvent.PassengerStatus.WAITING_FOR_RIDE_TO_START;
  }

  private WebsocketPassengerEvent.PassengerStatus rideStarted(OutboundPassengerMessage message) {
    OutboundPassengerMessage.RideStarted rideStarted = message.getRideStarted();

    simulation.broadcastPassengerSimulationEvent(
        WebsocketPassengerEvent.builder()
            .rideId("") // TODO: ?
            .passengerId(id)
            .startLocation(startCell)
            .endLocation(endCell)
            .driverId(rideStarted.getDriverId())
            .status(WebsocketPassengerEvent.PassengerStatus.RIDING)
            .build());

    return WebsocketPassengerEvent.PassengerStatus.RIDING;
  }

  @SuppressWarnings("unused")
  private WebsocketPassengerEvent.PassengerStatus rideEnded(OutboundPassengerMessage ignored) {
    simulation.broadcastPassengerSimulationEvent(
        WebsocketPassengerEvent.builder()
            .rideId("") // TODO: ?
            .passengerId(id)
            .startLocation(startCell)
            .endLocation(endCell)
            .driverId(driverId)
            .status(WebsocketPassengerEvent.PassengerStatus.DONE)
            .build());

    return WebsocketPassengerEvent.PassengerStatus.DONE;
  }

  private StateMachine<WebsocketPassengerEvent.PassengerStatus> passengerStateMachine() {
    StateMachine<WebsocketPassengerEvent.PassengerStatus> stateMachine =
        new StateMachine<>(WebsocketPassengerEvent.PassengerStatus.IDLE);

    stateMachine.withTerminalState(WebsocketPassengerEvent.PassengerStatus.FAIL);
    stateMachine.withTerminalState(WebsocketPassengerEvent.PassengerStatus.DONE);

    // send the req
    stateMachine.withState(
        WebsocketPassengerEvent.PassengerStatus.IDLE,
        LifecycleMessages.Initialization.class,
        this::init);

    // req success
    stateMachine.withState(
        WebsocketPassengerEvent.PassengerStatus.REQUESTING,
        OutboundPassengerMessage.class,
        OutboundPassengerMessage::hasDriverFound,
        this::driverFound);

    // req failure (terminal)
    stateMachine.withState(
        WebsocketPassengerEvent.PassengerStatus.REQUESTING,
        OutboundPassengerMessage.class,
        OutboundPassengerMessage::hasRideFailed,
        this::rideFailed);

    // ride started
    stateMachine.withState(
        WebsocketPassengerEvent.PassengerStatus.WAITING_FOR_RIDE_TO_START,
        OutboundPassengerMessage.class,
        OutboundPassengerMessage::hasRideStarted,
        this::rideStarted);

    // ride finished (terminal)
    stateMachine.withState(
        WebsocketPassengerEvent.PassengerStatus.RIDING,
        OutboundPassengerMessage.class,
        OutboundPassengerMessage::hasRideEnded,
        this::rideEnded);

    return stateMachine;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public boolean isDone() {
    return stateMachine.isAtTerminalState();
  }

  @Override
  public void apply(Object event) {
    stateMachine.apply(event);
  }

  @Override
  public boolean needReschedule() {
    return false;
  }
}
