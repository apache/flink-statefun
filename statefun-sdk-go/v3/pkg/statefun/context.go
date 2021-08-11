// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statefun

import (
	"context"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"sync"
	"time"
)

// A Context contains information about the current function invocation, such as the invoked
// function instance's and caller's Address. It is also used for side effects as a result of
// the invocation such as send messages to other functions or egresses, and provides access to
// AddressScopedStorage scoped to the current Address. This type is also a context.Context
// and can be used to ensure any spawned go routines do not outlive the current function
// invocation.
type Context interface {
	context.Context

	// Self is the current invoked function instance's Address.
	Self() Address

	// Caller is the caller function instance's Address, if applicable. This is nil
	// if the message was sent to this function via an ingress.
	Caller() *Address

	// Send forwards out a MessageBuilder to another function.
	Send(message MessageBuilder)

	// SendAfter forwards out a MessageBuilder to another function, after a specified time.Duration delay.
	SendAfter(delay time.Duration, message MessageBuilder)

	// SendAfterWithCancellationToken forwards out a MessageBuilder to another function,
	// after a specified time.Duration delay. The message is tagged with a non-empty,
	//unique token to attach to this message, to be used for message cancellation
	SendAfterWithCancellationToken(delay time.Duration, token CancellationToken, message MessageBuilder)

	// CancelDelayedMessage cancels a delayed message (a message that was send via SendAfterWithCancellationToken).
	// NOTE: this is a best-effort operation, since the message might have been already delivered.
	// If the message was delivered, this is a no-op operation.
	CancelDelayedMessage(token CancellationToken)

	// SendEgress forwards out an EgressBuilder to an egress.
	SendEgress(egress EgressBuilder)

	// Storage returns the AddressScopedStorage, providing access to stored values scoped to the
	// current invoked function instance's Address (which is obtainable using Self()).
	Storage() AddressScopedStorage
}

type statefunContext struct {
	sync.Mutex
	context.Context
	self     Address
	caller   *Address
	storage  *storage
	response *protocol.FromFunction_InvocationResponse
}

func (s *statefunContext) Storage() AddressScopedStorage {
	return s.storage
}

func (s *statefunContext) Self() Address {
	return s.self
}

func (s *statefunContext) Caller() *Address {
	return s.caller
}

func (s *statefunContext) Send(message MessageBuilder) {
	msg, err := message.ToMessage()

	if err != nil {
		panic(err)
	}

	invocation := &protocol.FromFunction_Invocation{
		Target:   msg.target,
		Argument: msg.typedValue,
	}

	s.Lock()
	s.response.OutgoingMessages = append(s.response.OutgoingMessages, invocation)
	s.Unlock()
}

func (s *statefunContext) SendAfter(delay time.Duration, message MessageBuilder) {
	msg, err := message.ToMessage()

	if err != nil {
		panic(err)
	}

	invocation := &protocol.FromFunction_DelayedInvocation{
		Target:    msg.target,
		Argument:  msg.typedValue,
		DelayInMs: delay.Milliseconds(),
	}

	s.Lock()
	s.response.DelayedInvocations = append(s.response.DelayedInvocations, invocation)
	s.Unlock()
}

func (s *statefunContext) SendAfterWithCancellationToken(delay time.Duration, token CancellationToken, message MessageBuilder) {
	msg, err := message.ToMessage()

	if err != nil {
		panic(err)
	}

	invocation := &protocol.FromFunction_DelayedInvocation{
		CancellationToken: token.String(),
		Target:            msg.target,
		Argument:          msg.typedValue,
		DelayInMs:         delay.Milliseconds(),
	}

	s.Lock()
	s.response.DelayedInvocations = append(s.response.DelayedInvocations, invocation)
	s.Unlock()
}

func (s *statefunContext) CancelDelayedMessage(token CancellationToken) {
	invocation := &protocol.FromFunction_DelayedInvocation{
		IsCancellationRequest: true,
		CancellationToken:     token.String(),
	}

	s.Lock()
	s.response.DelayedInvocations = append(s.response.DelayedInvocations, invocation)
	s.Unlock()
}

func (s *statefunContext) SendEgress(egress EgressBuilder) {
	msg, err := egress.toEgressMessage()

	if err != nil {
		panic(err)
	}

	s.Lock()
	s.response.OutgoingEgresses = append(s.response.OutgoingEgresses, msg)
	s.Unlock()
}
