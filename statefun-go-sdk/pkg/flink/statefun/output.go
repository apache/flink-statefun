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
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/messages"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/io"
	"google.golang.org/protobuf/proto"
	"time"
)

// Provides the effect tracker for a single StatefulFunction instance.
// The invocation's io context may be used to invoke other functions
// (including itself) and to send messages to egresses.
type Output interface {

	// Invokes another function with an input, identified by the target function's Address
	// and marshals the given message into an any.Any.
	Send(target Address, message proto.Message) error

	// Invokes another function with an input, identified by the target function's
	// FunctionType and unique id after a specified delay. This method is durable
	// and as such, the message will not be lost if the system experiences
	// downtime between when the message is sent and the end of the duration.
	// This method marshals the given message into an any.Any.
	SendAfter(target Address, duration time.Duration, message proto.Message) error

	// Sends an output to an EgressIdentifier.
	// This method marshals the given message into an any.Any.
	SendEgress(egress io.EgressIdentifier, message proto.Message) error
}

// output is the main effect tracker of the function invocation
// It tracks all responses that will be sent back to the
// Flink runtime after the full batch has been executed.
type output struct {
	invocations       []*messages.FromFunction_Invocation
	delayedInvocation []*messages.FromFunction_DelayedInvocation
	outgoingEgress    []*messages.FromFunction_EgressMessage
}

func (tracker *output) Send(target Address, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedState, err := internal.Marshall(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message for target %s", target.FunctionType.String())
	}

	invocation := &messages.FromFunction_Invocation{
		Target: &messages.Address{
			Namespace: target.FunctionType.Namespace,
			Type:      target.FunctionType.Type,
			Id:        target.Id,
		},
		Argument: packedState,
	}

	tracker.invocations = append(tracker.invocations, invocation)
	return nil
}

func (tracker *output) SendAfter(target Address, duration time.Duration, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedMessage, err := internal.Marshall(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message for delayed target %s", target.FunctionType.String())
	}

	delayedInvocation := &messages.FromFunction_DelayedInvocation{
		Target: &messages.Address{
			Namespace: target.FunctionType.Namespace,
			Type:      target.FunctionType.Type,
			Id:        target.Id,
		},
		DelayInMs: duration.Milliseconds(),
		Argument:  packedMessage,
	}

	tracker.delayedInvocation = append(tracker.delayedInvocation, delayedInvocation)
	return nil
}

func (tracker *output) SendEgress(egress io.EgressIdentifier, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to egress")
	}

	packedMessage, err := internal.Marshall(message)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message for egress %s", egress.String())
	}

	egressMessage := &messages.FromFunction_EgressMessage{
		EgressNamespace: egress.EgressNamespace,
		EgressType:      egress.EgressType,
		Argument:        packedMessage,
	}

	tracker.outgoingEgress = append(tracker.outgoingEgress, egressMessage)
	return nil
}
