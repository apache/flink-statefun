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

import com.google.common.base.Preconditions;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.simulator.model.WebsocketDriverEvent;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.LifecycleMessages;
import org.apache.flink.statefun.examples.ridesharing.simulator.simulation.engine.Simulatee;

public class Driver implements Simulatee {
  private final String driverId;
  private final DriverMessaging messaging;
  private final StateMachine<WebsocketDriverEvent.DriverStatus> stateMachine;
  private final int gridDimension;

  private int currentLocation;

  @Nullable private WebsocketDriverEvent.RideInformation rideInformation;

  Driver(String driverId, DriverMessaging messaging, int gridDimension, int startLocation) {
    this.driverId = driverId;
    this.messaging = messaging;
    this.gridDimension = gridDimension;
    this.currentLocation = startLocation;

    stateMachine = new StateMachine<>(WebsocketDriverEvent.DriverStatus.IDLE);

    // we don't have anything to do on initialization
    stateMachine.withState(
        WebsocketDriverEvent.DriverStatus.IDLE,
        LifecycleMessages.Initialization.class,
        unused -> WebsocketDriverEvent.DriverStatus.IDLE);

    // idle heartbeat
    stateMachine.withState(
        WebsocketDriverEvent.DriverStatus.IDLE, LifecycleMessages.TimeTick.class, this::heartbeat);

    // pickup request
    stateMachine.withState(
        WebsocketDriverEvent.DriverStatus.IDLE,
        OutboundDriverMessage.class,
        OutboundDriverMessage::hasPickupPassenger,
        this::pickupPassenger);

    stateMachine.withState(
        WebsocketDriverEvent.DriverStatus.PICKUP,
        LifecycleMessages.TimeTick.class,
        this::preformPickup);
    stateMachine.withState(
        WebsocketDriverEvent.DriverStatus.ENROUTE,
        LifecycleMessages.TimeTick.class,
        this::preformRoute);
  }

  /** send periodic heart beats when idle */
  private WebsocketDriverEvent.DriverStatus heartbeat(
      @SuppressWarnings("unused") LifecycleMessages.TimeTick ignored) {

    // notify application
    messaging.outgoingDriverEvent(
        InboundDriverMessage.newBuilder()
            .setDriverId(driverId)
            .setLocationUpdate(
                InboundDriverMessage.LocationUpdate.newBuilder().setCurrentGeoCell(currentLocation))
            .build());

    // notify the websocket
    messaging.broadcastDriverSimulationEvent(
        WebsocketDriverEvent.builder()
            .currentLocation(currentLocation)
            .driverId(driverId)
            .driverStatus(WebsocketDriverEvent.DriverStatus.IDLE)
            .ride(null)
            .build());

    return WebsocketDriverEvent.DriverStatus.IDLE;
  }

  /** receive a pickup command and start riding to the passenger */
  private WebsocketDriverEvent.DriverStatus pickupPassenger(OutboundDriverMessage message) {
    OutboundDriverMessage.PickupPassenger pickup = message.getPickupPassenger();

    // capture ride info from the pickup message
    this.rideInformation =
        WebsocketDriverEvent.RideInformation.builder()
            .passengerId(pickup.getRideId()) // TODO: fix this at the application side.
            .pickupLocation(pickup.getStartGeoLocation())
            .dropoffLocation(pickup.getEndGeoLocation())
            .build();

    // notify the websocket
    messaging.broadcastDriverSimulationEvent(
        WebsocketDriverEvent.builder()
            .ride(rideInformation)
            .driverStatus(WebsocketDriverEvent.DriverStatus.PICKUP)
            .driverId(driverId)
            .currentLocation(currentLocation)
            .build());

    return WebsocketDriverEvent.DriverStatus.PICKUP;
  }

  private WebsocketDriverEvent.DriverStatus preformPickup(
      @SuppressWarnings("unused") LifecycleMessages.TimeTick ignored) {
    Preconditions.checkState(rideInformation != null, "should have ride information.");
    if (currentLocation == rideInformation.getPickupLocation()) {
      // we have reached to the passenger, lets pick him up!

      messaging.broadcastDriverSimulationEvent(
          WebsocketDriverEvent.builder()
              .currentLocation(currentLocation)
              .driverId(driverId)
              .driverStatus(WebsocketDriverEvent.DriverStatus.ENROUTE)
              .ride(rideInformation)
              .build());

      messaging.outgoingDriverEvent(
          InboundDriverMessage.newBuilder()
              .setDriverId(driverId)
              .setRideStarted(InboundDriverMessage.RideStarted.getDefaultInstance())
              .build());

      return WebsocketDriverEvent.DriverStatus.ENROUTE;
    }

    // we need to advance toward the passenger
    int selfX = currentLocation / gridDimension;
    int selfY = currentLocation % gridDimension;
    final int passX = rideInformation.getPickupLocation() / gridDimension;
    final int passY = rideInformation.getPickupLocation() % gridDimension;

    if (ThreadLocalRandom.current().nextBoolean()) {
      //
      // advance in X
      //
      if (selfX > passX) {
        selfX--;
      } else if (selfX < passX) {
        selfX++;
      }
    } else {
      //
      // advance in Y
      //
      if (selfY > passY) {
        selfY--;
      } else if (selfY < passY) {
        selfY++;
      }
    }

    currentLocation = selfX * gridDimension + selfY;

    // send a heartbeat with our new location
    // notify application of our new location
    messaging.outgoingDriverEvent(
        InboundDriverMessage.newBuilder()
            .setDriverId(driverId)
            .setLocationUpdate(
                InboundDriverMessage.LocationUpdate.newBuilder().setCurrentGeoCell(currentLocation))
            .build());

    // notify the websocket
    messaging.broadcastDriverSimulationEvent(
        WebsocketDriverEvent.builder()
            .currentLocation(currentLocation)
            .driverId(driverId)
            .driverStatus(WebsocketDriverEvent.DriverStatus.PICKUP)
            .ride(rideInformation)
            .build());

    return WebsocketDriverEvent.DriverStatus.PICKUP;
  }

  private WebsocketDriverEvent.DriverStatus preformRoute(
      @SuppressWarnings("unused") LifecycleMessages.TimeTick ignored) {
    Preconditions.checkState(rideInformation != null, "should have ride information.");
    if (currentLocation == rideInformation.getDropoffLocation()) {
      // done!

      // notify websocket
      messaging.broadcastDriverSimulationEvent(
          WebsocketDriverEvent.builder()
              .currentLocation(currentLocation)
              .driverId(driverId)
              .driverStatus(
                  WebsocketDriverEvent.DriverStatus
                      .RIDE_COMPLETED) // TODO: should we send ride done?
              .ride(rideInformation)
              .build());

      // notify application
      messaging.outgoingDriverEvent(
          InboundDriverMessage.newBuilder()
              .setDriverId(driverId)
              .setRideEnded(
                  InboundDriverMessage.RideEnded.newBuilder()
                      .setRideId(rideInformation.getPassengerId()))
              .build());

      rideInformation = null;

      // we switch back to idle
      return WebsocketDriverEvent.DriverStatus.IDLE;
    }

    // we need to advance toward the passenger's dropoff location
    int selfX = currentLocation / gridDimension;
    int selfY = currentLocation % gridDimension;
    final int passX = rideInformation.getDropoffLocation() / gridDimension;
    final int passY = rideInformation.getDropoffLocation() % gridDimension;

    if (ThreadLocalRandom.current().nextBoolean()) {
      //
      // advance in X
      //
      if (selfX > passX) {
        selfX--;
      } else if (selfX < passX) {
        selfX++;
      }
    } else {
      //
      // advance in Y
      //
      if (selfY > passY) {
        selfY--;
      } else if (selfY < passY) {
        selfY++;
      }
    }

    currentLocation = selfX * gridDimension + selfY;

    // send a heartbeat with our new location
    // notify application of our new location
    messaging.outgoingDriverEvent(
        InboundDriverMessage.newBuilder()
            .setDriverId(driverId)
            .setLocationUpdate(
                InboundDriverMessage.LocationUpdate.newBuilder().setCurrentGeoCell(currentLocation))
            .build());

    // notify the websocket
    messaging.broadcastDriverSimulationEvent(
        WebsocketDriverEvent.builder()
            .currentLocation(currentLocation)
            .driverId(driverId)
            .driverStatus(WebsocketDriverEvent.DriverStatus.ENROUTE)
            .ride(rideInformation)
            .build());

    return WebsocketDriverEvent.DriverStatus.ENROUTE;
  }

  @Override
  public String id() {
    return driverId;
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
    return true; // the driver never rests.
  }
}
