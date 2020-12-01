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
	"google.golang.org/protobuf/types/known/anypb"
)

type Ride struct {
	Passenger statefun.State `state:"passenger"`
	Driver    statefun.State `state:"driver"`
}

func (r *Ride) Invoke(ctx context.Context, out statefun.Output, msg *anypb.Any) error {
	var join PassengerJoinsRide
	if err := msg.UnmarshalTo(&join); err == nil {
		return r.whenPassengerJoinsRide(out, &join)
	}

	var driverInCell DriverInCell
	if err := msg.UnmarshalTo(&driverInCell); err == nil {
		return r.whenGeoCellResponds(out, &driverInCell)
	}

	var reject DriverRejectsPickup
	if err := msg.UnmarshalTo(&reject); err == nil {
		return r.whenDriverRejectsPickup(out)
	}

	var joinsRide DriverJoinsRide
	if err := msg.UnmarshalTo(&joinsRide); err == nil {
		return r.whenDriverJoinsRide(ctx, out, &joinsRide)
	}

	var rideStarted RideStarted
	if err := msg.UnmarshalTo(&rideStarted); err == nil {
		return r.whenStartingRide(out, &rideStarted)
	}

	var rideEnded RideEnded
	if err := msg.UnmarshalTo(&rideEnded); err == nil {
		return r.whenEndingRide(out, &rideEnded)
	}

	return nil
}

// When a passenger joins a ride, we have to:
// 1. remember what passenger id
// 2. remember the starting location
// 3. contact the geo cell of the starting location
// and ask for a free driver
func (r *Ride) whenPassengerJoinsRide(out statefun.Output, join *PassengerJoinsRide) error {
	if err := r.Passenger.Set(join); err != nil {
		return err
	}

	cell := statefun.Address{
		FunctionType: GeoCellType,
		Id:           fmt.Sprint(join.StartGeoCell),
	}

	return out.Send(cell, &GetDriver{})
}

// Geo cell responds, it might respond with: - there is no driver, in that case we fail the ride -
// there is a driver, let's ask them to pickup the rPassenger.
func (r *Ride) whenGeoCellResponds(out statefun.Output, in *DriverInCell) error {
	var request PassengerJoinsRide
	if _, err := r.Passenger.Get(&request); err != nil {
		return err
	}

	if len(in.DriverId) == 0 {
		// no free gcDrivers in this geo cell, at this example we just fail the dRide
		// but we can imagine that this is where we will expand our search to near geo cells
		passenger := statefun.Address{
			FunctionType: PassengerType,
			Id:           request.PassengerId,
		}

		// by clearing our state, we essentially delete this instance of the dRide actor
		r.Passenger.Clear()
		return out.Send(passenger, &RideFailed{})
	}

	driver := statefun.Address{
		FunctionType: DriverType,
		Id:           in.DriverId,
	}

	pickup := &PickupPassenger{
		DriverId:           in.DriverId,
		PassengerId:        request.PassengerId,
		PassengerStartCell: request.StartGeoCell,
		PassengerEndCell:   request.EndGeoCell,
	}

	return out.Send(driver, pickup)
}

// A driver might not be free, or for some other reason they cannot take this ride,
// so we try another rDriver in that cell.
func (r *Ride) whenDriverRejectsPickup(out statefun.Output) error {
	// try another rDriver, realistically we need to pass in a list of 'banned' gcDrivers,
	// so that the GeoCell will not offer us these gcDrivers again, but in this example
	// if a rDriver rejects a dRide, it means that he is currently busy (and it would soon delete
	// itself from the geo cell)

	var passengerJoinsRide PassengerJoinsRide
	if _, err := r.Passenger.Get(&passengerJoinsRide); err != nil {
		return err
	}

	startGeoCell := statefun.Address{
		FunctionType: GeoCellType,
		Id:           fmt.Sprint(passengerJoinsRide.StartGeoCell),
	}

	return out.Send(startGeoCell, &GetDriver{})
}

func (r *Ride) whenDriverJoinsRide(ctx context.Context, out statefun.Output, joinsRide *DriverJoinsRide) error {
	driver := CurrentDriver{
		DriverId: statefun.Caller(ctx).Id,
	}

	if err := r.Driver.Set(&driver); err != nil {
		return err
	}

	request := PassengerJoinsRide{}
	if _, err := r.Passenger.Get(&request); err != nil {
		return err
	}

	passenger := statefun.Address{
		FunctionType: PassengerType,
		Id:           request.PassengerId,
	}

	return out.Send(passenger, joinsRide)
}

// A driver has successfully picked up the passenger
func (r *Ride) whenStartingRide(out statefun.Output, started *RideStarted) error {
	request := PassengerJoinsRide{}
	if _, err := r.Passenger.Get(&request); err != nil {
		return err
	}

	passengerAddress := statefun.Address{
		FunctionType: PassengerType,
		Id:           request.PassengerId,
	}

	return out.Send(passengerAddress, started)
}

// The driver has successfully reached the destination
func (r *Ride) whenEndingRide(out statefun.Output, ended *RideEnded) error {
	var request PassengerJoinsRide
	if _, err := r.Passenger.Get(&request); err != nil {
		return err
	}

	passengerAddress := statefun.Address{
		FunctionType: PassengerType,
		Id:           request.PassengerId,
	}

	r.Passenger.Clear()
	r.Driver.Clear()

	return out.Send(passengerAddress, ended)
}
