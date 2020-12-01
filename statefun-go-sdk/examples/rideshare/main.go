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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/io"
	"log"
	"net/http"
)

var (
	GeoCellType = statefun.FunctionType{
		Namespace: "ridesharing",
		Type:      "geocell",
	}

	DriverType = statefun.FunctionType{
		Namespace: "ridesharing",
		Type:      "driver",
	}

	RideType = statefun.FunctionType{
		Namespace: "ridesharing",
		Type:      "ride",
	}

	PassengerType = statefun.FunctionType{
		Namespace: "ridesharing",
		Type:      "passenger",
	}

	ToPassengerEgress = io.EgressIdentifier{
		EgressNamespace: "ridesharing",
		EgressType:      "to-passenger",
	}

	ToDriverEgress = io.EgressIdentifier{
		EgressNamespace: "ridesharing",
		EgressType:      "to-driver",
	}
)

func main() {
	registry := statefun.NewFunctionRegistry()

	if err := registry.RegisterFunction(DriverType, &Driver{}); err != nil {
		log.Fatal(err)
	}

	if err := registry.RegisterFunction(GeoCellType, &GeoCell{}); err != nil {
		log.Fatal(err)
	}

	if err := registry.RegisterFunction(PassengerType, &Passenger{}); err != nil {
		log.Fatal(err)
	}

	if err := registry.RegisterFunction(RideType, &Ride{}); err != nil {
		log.Fatal(err)
	}

	http.Handle("/statefun", registry)
	_ = http.ListenAndServe(":8000", nil)
}
