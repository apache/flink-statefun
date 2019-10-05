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

package com.ververica.statefun.examples.ridesharing;

import com.ververica.statefun.examples.ridesharing.generated.DriverInCell;
import com.ververica.statefun.examples.ridesharing.generated.DriverJoinsRide;
import com.ververica.statefun.examples.ridesharing.generated.DriverRejectsPickup;
import com.ververica.statefun.examples.ridesharing.generated.GetDriver;
import com.ververica.statefun.examples.ridesharing.generated.PassengerJoinsRide;
import com.ververica.statefun.examples.ridesharing.generated.PickupPassenger;
import com.ververica.statefun.examples.ridesharing.generated.RideEnded;
import com.ververica.statefun.examples.ridesharing.generated.RideFailed;
import com.ververica.statefun.examples.ridesharing.generated.RideStarted;
import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.match.MatchBinder;
import com.ververica.statefun.sdk.match.StatefulMatchFunction;
import com.ververica.statefun.sdk.state.PersistedValue;

final class FnRide extends StatefulMatchFunction {

  static final FunctionType TYPE = new FunctionType(Identifiers.NAMESPACE, "ride");

  @Persisted
  private final PersistedValue<PassengerJoinsRide> passenger =
      PersistedValue.of("passenger", PassengerJoinsRide.class);

  @Persisted
  private final PersistedValue<String> driver = PersistedValue.of("driver", String.class);

  public void configure(MatchBinder binder) {
    binder
        .predicate(PassengerJoinsRide.class, this::whenPassengerJoins)
        .predicate(DriverInCell.class, this::whenGeoCellResponds)
        .predicate(DriverRejectsPickup.class, this::whenDriverRejectsPickup)
        .predicate(DriverJoinsRide.class, this::whenDriverJoins)
        .predicate(RideStarted.class, this::whenRideHasStarted)
        .predicate(RideEnded.class, this::whenRideHasEnded);
  }

  /**
   * When a user joins a ride, we have to: 1. remember that user id 2. remember the starting
   * location of that ride 3. contact the geo cell of the starting location, and ask for a free
   * driver
   */
  private void whenPassengerJoins(Context context, PassengerJoinsRide in) {
    final String cellKey = String.valueOf(in.getStartGeoCell());
    passenger.set(in);

    context.send(FnGeoCell.TYPE, cellKey, GetDriver.getDefaultInstance());
  }

  /**
   * Geo cell responds, it might respond with: - there is no driver, in that case we fail the ride -
   * there is a driver, let's ask them to pickup the passenger.
   */
  private void whenGeoCellResponds(Context context, DriverInCell in) {
    final String driverId = in.getDriverId();
    final PassengerJoinsRide rideRequest = passenger.get();
    if (driverId != null && !driverId.isEmpty()) {
      context.send(
          FnDriver.TYPE,
          driverId,
          PickupPassenger.newBuilder()
              .setPassengerId(rideRequest.getPassengerId())
              .setPassengerStartCell(rideRequest.getStartGeoCell())
              .setPassengerEndCell(rideRequest.getEndGeoCell())
              .build());
      return;
    }
    // no free drivers in this geo cell, at this example we just fail the ride
    // but we can imagine that this is where we will expand our search to near geo cells
    context.send(FnPassenger.TYPE, rideRequest.getPassengerId(), RideFailed.getDefaultInstance());

    // by clearing our state, we essentially delete this instance of the ride actor
    passenger.clear();
  }

  /**
   * A driver might not be free, or for some other reason they can't take this ride, so we try
   * another driver in that cell.
   */
  @SuppressWarnings("unused")
  private void whenDriverRejectsPickup(Context context, DriverRejectsPickup ignored) {
    // try another driver, realistically we need to pass in a list of 'banned' drivers,
    // so that the GeoCell will not offer us these drivers again, but in this example
    // if a driver rejects a ride, it means that he is currently busy (and it would soon delete
    // itself from the geo cell)
    final int startGeoCell = passenger.get().getStartGeoCell();
    String cellKey = String.valueOf(startGeoCell);
    context.send(FnGeoCell.TYPE, cellKey, GetDriver.getDefaultInstance());
  }

  /** A driver has taken this ride. */
  private void whenDriverJoins(Context context, DriverJoinsRide driverJoinRide) {
    driver.set(context.caller().id());
    context.send(FnPassenger.TYPE, passenger.get().getPassengerId(), driverJoinRide);
  }

  /** A driver has successfully picked up the passenger */
  private void whenRideHasStarted(Context context, RideStarted rideStarted) {
    context.send(FnPassenger.TYPE, passenger.get().getPassengerId(), rideStarted);
  }

  /** The driver has successfully reached the destination. */
  private void whenRideHasEnded(Context context, RideEnded rideEnded) {
    context.send(FnPassenger.TYPE, passenger.get().getPassengerId(), rideEnded);
    passenger.clear();
    driver.clear();
  }
}
