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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.statefun.examples.ridesharing.generated.DriverJoinsRide;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundPassengerMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.InboundPassengerMessage.RequestRide;
import org.apache.flink.statefun.examples.ridesharing.generated.OutboundPassengerMessage;
import org.apache.flink.statefun.examples.ridesharing.generated.PassengerJoinsRide;
import org.apache.flink.statefun.examples.ridesharing.generated.RideEnded;
import org.apache.flink.statefun.examples.ridesharing.generated.RideFailed;
import org.apache.flink.statefun.examples.ridesharing.generated.RideStarted;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;

public class FnPassenger extends StatefulMatchFunction {

  static final FunctionType TYPE = new FunctionType(Identifiers.NAMESPACE, "passenger");

  @Override
  public void configure(MatchBinder binder) {
    binder
        .predicate(
            InboundPassengerMessage.class,
            InboundPassengerMessage::hasRequestRide,
            this::whenRideIsRequested)
        .predicate(DriverJoinsRide.class, this::whenDriverJoins)
        .predicate(RideFailed.class, this::whenRideFails)
        .predicate(RideStarted.class, this::whenRideHasStarted)
        .predicate(RideEnded.class, this::whenRideHasEnded);
  }

  private void whenRideIsRequested(Context context, InboundPassengerMessage request) {
    String passengerID = context.self().id();
    String rideId = "ride-" + ThreadLocalRandom.current().nextLong();

    RequestRide rideRequest = request.getRequestRide();
    PassengerJoinsRide joinRide =
        PassengerJoinsRide.newBuilder()
            .setPassengerId(passengerID)
            .setStartGeoCell(rideRequest.getStartGeoCell())
            .setEndGeoCell(rideRequest.getEndGeoCell())
            .build();

    context.send(FnRide.TYPE, rideId, joinRide);
  }

  private void whenRideHasEnded(Context context, RideEnded ignored) {
    final OutboundPassengerMessage out =
        OutboundPassengerMessage.newBuilder()
            .setPassengerId(context.self().id())
            .setRideEnded(OutboundPassengerMessage.RideEnded.newBuilder().build())
            .build();

    context.send(Identifiers.TO_PASSENGER_EGRESS, out);
  }

  private void whenRideHasStarted(Context context, RideStarted rideStarted) {
    final OutboundPassengerMessage out =
        OutboundPassengerMessage.newBuilder()
            .setPassengerId(context.self().id())
            .setRideStarted(
                OutboundPassengerMessage.RideStarted.newBuilder()
                    .setDriverId(rideStarted.getDriverId())
                    .build())
            .build();

    context.send(Identifiers.TO_PASSENGER_EGRESS, out);
  }

  private void whenDriverJoins(Context context, DriverJoinsRide message) {
    final OutboundPassengerMessage out =
        OutboundPassengerMessage.newBuilder()
            .setPassengerId(context.self().id())
            .setDriverFound(
                OutboundPassengerMessage.DriverHasBeenFound.newBuilder()
                    .setDriverId(message.getDriverId())
                    .setDriverGeoCell(message.getDriverLocation())
                    .build())
            .build();

    context.send(Identifiers.TO_PASSENGER_EGRESS, out);
  }

  private void whenRideFails(Context context, RideFailed rideFailed) {
    final OutboundPassengerMessage out =
        OutboundPassengerMessage.newBuilder()
            .setPassengerId(context.self().id())
            .setRideFailed(
                OutboundPassengerMessage.RideFailed.newBuilder()
                    .setRideId(rideFailed.getRideId())
                    .build())
            .build();

    context.send(Identifiers.TO_PASSENGER_EGRESS, out);
  }
}
