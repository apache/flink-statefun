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

package main

import (
	"fmt"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"strconv"
	"time"
)

var (
	State = statefun.ValueSpec{
		Name:      "state",
		ValueType: statefun.Int64Type,
	}
)

func CommandInterpreterFn(ctx statefun.Context, message statefun.Message) error {
	if message.Is(sourceCommandsType) {
		cmds := &SourceCommand{}
		if err := message.As(sourceCommandsType, cmds); err != nil {
			return fmt.Errorf("failed to deserialize source commonds: %w", err)
		}
		return interpret(ctx, cmds.GetCommands())
	} else if message.Is(commandsType) {
		cmds := &Commands{}
		if err := message.As(commandsType, cmds); err != nil {
			return fmt.Errorf("failed to deserialize commonds: %w", err)
		}
		return interpret(ctx, cmds)
	}

	return fmt.Errorf("unrecognized message type %v", message.ValueTypeName())
}

func interpret(ctx statefun.Context, cmds *Commands) error {
	for _, cmd := range cmds.GetCommand() {
		if cmd.GetIncrement() != nil {
			modifyState(ctx)
		} else if cmd.GetSend() != nil {
			send(ctx, cmd.GetSend())
		} else if cmd.GetSendAfter() != nil {
			sendAfter(ctx, cmd.GetSendAfter())
		} else if cmd.GetSendEgress() != nil {
			sendEgress(ctx)
		} else if cmd.GetVerify() != nil {
			verify(ctx, cmd.GetVerify())
		}
	}

	return nil
}

func modifyState(ctx statefun.Context) {
	var value int64
	ctx.Storage().Get(State, &value)
	value += 1
	ctx.Storage().Set(State, value)
}

func send(ctx statefun.Context, cmd *Command_Send) {
	target := statefun.Address{
		FunctionType: commandFn,
		Id:           fmt.Sprint(cmd.GetTarget()),
	}

	message := statefun.MessageBuilder{
		Target:    target,
		Value:     cmd.GetCommands(),
		ValueType: commandsType,
	}

	ctx.Send(message)
}

func sendAfter(ctx statefun.Context, cmd *Command_SendAfter) {
	target := statefun.Address{
		FunctionType: commandFn,
		Id:           fmt.Sprint(cmd.GetTarget()),
	}

	message := statefun.MessageBuilder{
		Target:    target,
		Value:     cmd.GetCommands(),
		ValueType: commandsType,
	}

	ctx.SendAfter(time.Duration(1), message)
}

func sendEgress(ctx statefun.Context) {
	message := statefun.GenericEgressBuilder{
		Target:    discardEgress,
		Value:     "discarded-message",
		ValueType: statefun.StringType,
	}

	ctx.SendEgress(message)
}

func verify(ctx statefun.Context, verify *Command_Verify) {
	id, _ := strconv.Atoi(ctx.Self().Id)

	var actual int64
	ctx.Storage().Get(State, &actual)

	message := statefun.GenericEgressBuilder{
		Target: verificationEgress,
		Value: &VerificationResult{
			Id:       int32(id),
			Expected: verify.GetExpected(),
			Actual:   actual,
		},
		ValueType: verificationType,
	}

	ctx.SendEgress(message)
}
