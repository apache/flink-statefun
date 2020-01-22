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
package org.apache.flink.statefun.examples.ridesharing;

import org.apache.flink.statefun.examples.ridesharing.generated.DriverJoinsRide;
import org.apache.flink.statefun.examples.ridesharing.generated.DriverRejectsPickup;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.JoinCell;
import org.apache.flink.statefun.examples.ridesharing.generated.LeaveCell;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundDriverMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.PickupPassenger;
import org.apache.flink.statefun.examples.ridesharing.generated.RideEnded;
import org.apache.flink.statefun.examples.ridesharing.generated.RideStarted;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class FnDriver extends StatefulMatchFunction {

  static final FunctionType TYPE = new FunctionType(Identifiers.NAMESPACE, "driver");

  @Persisted
  private final PersistedValue<String> currentRideId = PersistedValue.of("ride", String.class);

  @Persisted
  private final PersistedValue<Integer> location = PersistedValue.of("location", Integer.class);

  @Override
  public void configure(MatchBinder binder) {
    binder
        .predicate(PickupPassenger.class, this::whenPickupIsNeeded)
        .predicate(
            InboundDriverMessage.class,
            InboundDriverMessage::hasRideStarted,
            this::whenRideHasStarted)
        .predicate(
            InboundDriverMessage.class, InboundDriverMessage::hasRideEnded, this::whenRideHasEnded)
        .predicate(
            InboundDriverMessage.class,
            InboundDriverMessage::hasLocationUpdate,
            this::whenLocationIsUpdated);
  }

  private void whenPickupIsNeeded(Context context, PickupPassenger pickupPassenger) {
    if (isTaken()) {
      // this driver is currently in a ride, and therefore can't take any more
      // passengers.
      context.reply(
          DriverRejectsPickup.newBuilder()
              .setDriverId(context.self().id())
              .setRideId(context.caller().id())
              .build());
      return;
    }
    // We are called by the ride function, so we remember it's id for future communication.
    currentRideId.set(context.caller().id());

    // We also need to unregister ourselves from the current geo cell we belong to.
    final int currentLocation =
        location.getOrDefault(0); // drivers should have a location at this point.
    context.send(FnGeoCell.TYPE, String.valueOf(currentLocation), LeaveCell.getDefaultInstance());

    // reply to the ride, saying we are taking this passenger
    context.reply(
        DriverJoinsRide.newBuilder()
            .setDriverId(context.self().id())
            .setDriverLocation(currentLocation)
            .build());

    // also send a command to the physical driver to pickup the passenger
    context.send(
        Identifiers.TO_OUTBOUND_DRIVER,
        OutboundDriverMessage.newBuilder()
            .setDriverId(context.self().id())
            .setPickupPassenger(
                OutboundDriverMessage.PickupPassenger.newBuilder()
                    .setRideId(pickupPassenger.getPassengerId())
                    .setStartGeoLocation(pickupPassenger.getPassengerStartCell())
                    .setEndGeoLocation(pickupPassenger.getPassengerEndCell())
                    .build())
            .build());
  }

  private void whenRideHasStarted(Context context, InboundDriverMessage ignored) {
    context.send(
        FnRide.TYPE,
        currentRideId.get(),
        RideStarted.newBuilder()
            .setDriverId(context.self().id())
            .setDriverGeoCell(location.get())
            .build());
  }

  private void whenRideHasEnded(Context context, InboundDriverMessage ignored) {
    context.send(FnRide.TYPE, currentRideId.get(), RideEnded.getDefaultInstance());
    currentRideId.clear();

    // register at the current location as free driver.
    Integer currentLocation = location.get();
    context.send(FnGeoCell.TYPE, String.valueOf(currentLocation), JoinCell.getDefaultInstance());
  }

  private void whenLocationIsUpdated(Context context, InboundDriverMessage locationUpdate) {
    final int updated = locationUpdate.getLocationUpdate().getCurrentGeoCell();
    final int last = location.getOrDefault(-1);
    if (last == -1) {
      // this is the first time this driver gets a location update.
      // so we notify the relevant geo cell function.
      location.set(updated);
      context.send(FnGeoCell.TYPE, String.valueOf(updated), JoinCell.getDefaultInstance());
      return;
    }
    if (last == updated) {
      return;
    }
    location.set(updated);
  }

  private boolean isTaken() {
    String rideId = currentRideId.get();
    return rideId != null;
  }
}
