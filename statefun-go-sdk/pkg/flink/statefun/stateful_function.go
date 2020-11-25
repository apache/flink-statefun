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

package statefun

import (
	"context"
	"google.golang.org/protobuf/types/known/anypb"
)

// StatefulFunction is a user-defined function that can be invoked with a given input.
// This is the primitive building block for a Stateful Functions application.
//
// Each individual function is a uniquely invokable "instance" of a FunctionType.
// Each function is identified by an Address, representing the function's
// unique id (a string) within its type. From a user's perspective, it would seem
// as if for each unique function id, there exists a stateful instance of the function
// that is always available to be invoked within an application.
//
// An individual function can be invoked with arbitrary input form any other
// function (including itself), or routed form an ingress via a Router. To
// execute a function, the caller simply needs to know the Address of the target
// function. As a result of invoking a StatefulFunction, the function may continue
// to execute other functions, modify its state, or send messages to egresses
// addressed by an egress identifier.
//
// Each individual function instance may have state that is maintained by the system,
// providing exactly-once guarantees. State values are configured by adding fields to
// the interface implementation of type State with the appropriate tags.
//
//	type Greeter struct {
//		SeenCount State `state:"seen_count"`
//	}
//
//	func (g greeter) Invoke(ctx context.Context, output Output, msg *anypb.Any) error {
//
//	}
type StatefulFunction interface {

	// Invoke this function with the given input.
	Invoke(ctx context.Context, output Output, msg *anypb.Any) error
}
