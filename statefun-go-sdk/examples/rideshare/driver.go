// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/io"
	"google.golang.org/protobuf/types/known/anypb"
)

type Driver struct {
	Ride     statefun.State `state:"ride"`
	Location statefun.State `state:"location"`
}

func (d *Driver) Invoke(ctx context.Context, out statefun.Output, msg *anypb.Any) error {
	pickup := &PickupPassenger{}
	if err := msg.UnmarshalTo(pickup); err == nil {
		return d.pickupNeeded(ctx, out, pickup)
	}

	driverMessage := InboundDriverMessage{}
	if err := msg.UnmarshalTo(&driverMessage); err == nil {
		if rideStarted := driverMessage.GetRideStarted(); rideStarted != nil {
			return d.whenRideStarted(ctx, out)
		}

		if rideEnded := driverMessage.GetRideEnded(); rideEnded != nil {
			return d.whenRideEnded(out)
		}

		if locationUpdate := driverMessage.GetLocationUpdate(); locationUpdate != nil {
			return d.whenLocationUpdated(out, locationUpdate)
		}
	}

	return nil
}

func (d *Driver) pickupNeeded(ctx context.Context, out statefun.Output, pickup *PickupPassenger) error {
	var ride CurrentRide

	if exists, err := d.Ride.Get(&ride); err != nil {
		return err
	} else if exists {
		// this driver is currently in a ride and therefore cannot take
		// anymore passengers

		driverId := statefun.Self(ctx).Id
		ride := statefun.Caller(ctx)

		return out.Send(*ride, &DriverRejectsPickup{
			DriverId: driverId,
			RideId:   ride.Id,
		})
	}

	// We are called by the ride function, so we remember its id
	// for future communication.

	ride.RideId = statefun.Caller(ctx).Id
	if err := d.Ride.Set(&ride); err != nil {
		return err
	}

	var location CurrentLocation
	if _, err := d.Location.Get(&location); err != nil {
		return err
	}

	// We also need to unregister ourselves from the current
	// geo cell we belong to
	cell := statefun.Address{
		FunctionType: GeoCellType,
		Id:           fmt.Sprint(location.Location),
	}

	if err := out.Send(cell, &LeaveCell{}); err != nil {
		return err
	}

	// Reply to the ride saying we are taking this passenger

	driverId := statefun.Self(ctx).Id
	join := &DriverJoinsRide{
		DriverId:       driverId,
		PassengerId:    ride.RideId,
		DriverLocation: location.Location,
	}

	if err := out.Send(*statefun.Caller(ctx), join); err != nil {
		return err
	}

	record := io.KafkaRecord{
		Topic: "to-driver",
		Key:   driverId,
		Value: &OutboundDriverMessage{
			DriverId: driverId,
			Message: &OutboundDriverMessage_PickupPassenger_{
				PickupPassenger: &OutboundDriverMessage_PickupPassenger{
					RideId:           ride.RideId,
					StartGeoLocation: pickup.PassengerStartCell,
					EndGeoLocation:   pickup.PassengerEndCell,
				},
			},
		},
	}

	if message, err := record.ToMessage(); err != nil {
		return err
	} else {
		return out.SendEgress(ToDriverEgress, message)
	}
}

func (d *Driver) whenRideStarted(ctx context.Context, out statefun.Output) error {
	var location CurrentLocation
	if _, err := d.Location.Get(&location); err != nil {
		return err
	}

	var ride CurrentRide
	if _, err := d.Ride.Get(&ride); err != nil {
		return err
	}

	started := RideStarted{
		DriverId:      statefun.Self(ctx).Id,
		DriverGeoCell: location.Location,
	}

	address := statefun.Address{
		FunctionType: RideType,
		Id:           ride.RideId,
	}

	return out.Send(address, &started)
}

func (d *Driver) whenRideEnded(out statefun.Output) error {
	var ride CurrentRide
	if _, err := d.Ride.Get(&ride); err != nil {
		return nil
	}

	rideAddress := statefun.Address{
		FunctionType: RideType,
		Id:           ride.RideId,
	}

	if err := out.Send(rideAddress, &RideEnded{}); err != nil {
		return err
	}

	d.Ride.Clear()

	// register the driver as free at the current dLocation
	var location CurrentLocation
	if _, err := d.Location.Get(&location); err != nil {
		return err
	}

	geoCell := statefun.Address{
		FunctionType: GeoCellType,
		Id:           fmt.Sprint(location.Location),
	}

	return out.Send(geoCell, &JoinCell{})
}

func (d *Driver) whenLocationUpdated(runtime statefun.Output, update *InboundDriverMessage_LocationUpdate) error {
	var location CurrentLocation
	if exists, err := d.Location.Get(&location); err != nil {
		return err
	} else if !exists {
		// this is the first time this driver gets
		// a location update so we notify the relevant
		// geo cell function.

		location.Location = update.CurrentGeoCell

		geoCell := statefun.Address{
			FunctionType: GeoCellType,
			Id:           fmt.Sprint(location.Location),
		}

		return runtime.Send(geoCell, &location)
	}

	if location.Location == update.CurrentGeoCell {
		return nil
	}

	location.Location = update.CurrentGeoCell
	return d.Location.Set(&location)
}
