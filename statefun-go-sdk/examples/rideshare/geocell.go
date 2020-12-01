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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun"
	"google.golang.org/protobuf/types/known/anypb"
)

type GeoCell struct {
	Drivers statefun.State `state:"drivers"`
}

func (g *GeoCell) Invoke(ctx context.Context, out statefun.Output, msg *anypb.Any) error {
	caller := *statefun.Caller(ctx)

	cell := JoinCell{}
	if err := msg.UnmarshalTo(&cell); err == nil {
		return g.addDriver(caller.Id)
	}

	leave := LeaveCell{}
	if err := msg.UnmarshalTo(&leave); err == nil {
		return g.removeDriver(caller.Id)
	}

	get := GetDriver{}
	if err := msg.UnmarshalTo(&get); err == nil {
		return g.getDriver(caller, out)
	}

	return nil
}

func (g *GeoCell) addDriver(driverId string) error {
	var state GeoCellState
	if _, err := g.Drivers.Get(&state); err != nil {
		return err
	}

	state.DriverId[driverId] = true

	return g.Drivers.Set(&state)
}

func (g *GeoCell) removeDriver(driverId string) error {
	var state GeoCellState
	if _, err := g.Drivers.Get(&state); err != nil {
		return err
	}

	delete(state.DriverId, driverId)

	return g.Drivers.Set(&state)
}

func (g *GeoCell) getDriver(caller statefun.Address, out statefun.Output) error {
	var state GeoCellState
	if _, err := g.Drivers.Get(&state); err != nil {
		return err
	}

	for driverId := range state.DriverId {
		response := &DriverInCell{
			DriverId: driverId,
		}

		return out.Send(caller, response)
	}

	return out.Send(caller, &DriverInCell{})
}
