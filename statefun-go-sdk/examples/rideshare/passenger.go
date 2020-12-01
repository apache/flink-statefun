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
	"math/rand"
)

type Passenger struct{}

func (p *Passenger) Invoke(ctx context.Context, out statefun.Output, msg *anypb.Any) error {
	passengerId := statefun.Self(ctx).Id

	var inbound InboundPassengerMessage
	if err := msg.UnmarshalTo(&inbound); err == nil {
		request := inbound.GetRequestRide()
		return p.whenRideRequested(passengerId, out, request)
	}

	var driverJoin DriverJoinsRide
	if err := msg.UnmarshalTo(&driverJoin); err == nil {
		return p.whenDriverJoins(passengerId, out, &driverJoin)
	}

	var rideFailed RideFailed
	if err := msg.UnmarshalTo(&rideFailed); err == nil {
		return p.whenRideFailed(passengerId, out, &rideFailed)
	}

	var rideStarted RideStarted
	if err := msg.UnmarshalTo(&rideStarted); err == nil {
		return p.whenRideHasStarted(passengerId, out, &rideStarted)
	}

	var rideEnded RideEnded
	if err := msg.UnmarshalTo(&rideEnded); err == nil {
		return p.whenRideHasEnded(passengerId, out)
	}

	return nil
}

func (p *Passenger) whenRideRequested(passengerId string, out statefun.Output, request *InboundPassengerMessage_RequestRide) error {
	rideId := fmt.Sprintf("ride-%d", rand.Uint64())

	joinRide := &PassengerJoinsRide{
		PassengerId:  passengerId,
		StartGeoCell: request.GetStartGeoCell(),
		EndGeoCell:   request.GetEndGeoCell(),
	}

	ride := statefun.Address{
		FunctionType: RideType,
		Id:           rideId,
	}

	return out.Send(ride, joinRide)
}

func (p *Passenger) whenDriverJoins(passengerId string, out statefun.Output, driverJoin *DriverJoinsRide) error {
	record := io.KafkaRecord{
		Topic: "to-rPassenger",
		Key:   passengerId,
		Value: &OutboundPassengerMessage{
			PassengerId: passengerId,
			Message: &OutboundPassengerMessage_DriverFound{
				DriverFound: &OutboundPassengerMessage_DriverHasBeenFound{
					DriverId:      driverJoin.DriverId,
					DriverGeoCell: driverJoin.DriverLocation,
				},
			},
		},
	}

	if message, err := record.ToMessage(); err != nil {
		return err
	} else {
		return out.SendEgress(ToPassengerEgress, message)
	}
}

func (p *Passenger) whenRideFailed(passengerId string, out statefun.Output, rideFailed *RideFailed) error {
	record := io.KafkaRecord{
		Topic: "to-passenger",
		Key:   passengerId,
		Value: &OutboundPassengerMessage{
			PassengerId: passengerId,
			Message: &OutboundPassengerMessage_RideFailed_{
				RideFailed: &OutboundPassengerMessage_RideFailed{
					RideId: rideFailed.RideId,
				},
			},
		},
	}

	if message, err := record.ToMessage(); err != nil {
		return err
	} else {
		return out.SendEgress(ToPassengerEgress, message)
	}
}

func (p *Passenger) whenRideHasStarted(passengerId string, out statefun.Output, started *RideStarted) error {
	record := io.KafkaRecord{
		Topic: "to-passenger",
		Key:   passengerId,
		Value: &OutboundPassengerMessage{
			PassengerId: passengerId,
			Message: &OutboundPassengerMessage_RideStarted_{
				RideStarted: &OutboundPassengerMessage_RideStarted{
					DriverId: started.DriverId,
				},
			},
		},
	}

	if message, err := record.ToMessage(); err != nil {
		return err
	} else {
		return out.SendEgress(ToPassengerEgress, message)
	}
}

func (p *Passenger) whenRideHasEnded(passengerId string, out statefun.Output) error {
	record := io.KafkaRecord{
		Topic: "to-passenger",
		Key:   passengerId,
		Value: &OutboundPassengerMessage{
			PassengerId: passengerId,
			Message: &OutboundPassengerMessage_RideEnded_{
				RideEnded: &OutboundPassengerMessage_RideEnded{},
			},
		},
	}

	if message, err := record.ToMessage(); err != nil {
		return err
	} else {
		return out.SendEgress(ToPassengerEgress, message)
	}
}
