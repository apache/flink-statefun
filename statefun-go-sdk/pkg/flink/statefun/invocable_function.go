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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/messages"
	"google.golang.org/protobuf/proto"
)

type invokableFunction struct {
	function StatefulFunction
	states   map[string]*persistedValue
}

func (invokable *invokableFunction) invoke(ctx context.Context, request *messages.ToFunction) (*messages.FromFunction, error) {
	invocations := request.GetInvocation()
	if invocations == nil {
		return nil, errors.BadRequest("missing invocations for batch")
	}

	if missing := setRegisteredStates(request.GetInvocation().State, invokable.states); missing != nil {
		return missing, nil
	}

	out := &output{}
	self := addressFromInternal(invocations.Target)
	ctx = context.WithValue(ctx, selfKey, self)

	for _, invocation := range invocations.Invocations {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			caller := addressFromInternal(invocation.Caller)
			ctx = context.WithValue(ctx, callerKey, caller)
			err := invokable.function.Invoke(ctx, out, (*invocation).Argument)

			if err != nil {
				return nil, errors.Wrap(err, "failed to execute invokable %s", self.String())
			}
		}
	}

	return fromFunction(invokable.states, out)
}

func setRegisteredStates(
	providedStates []*messages.ToFunction_PersistedValue,
	statesSpecs map[string]*persistedValue) *messages.FromFunction {

	for _, state := range statesSpecs {
		state.updated = false
		state.value = nil
	}

	stateSet := make(map[string]bool, len(providedStates))
	for _, state := range providedStates {
		stateSet[state.StateName] = true
	}

	var missingValues []*messages.FromFunction_PersistedValueSpec
	for name, value := range statesSpecs {
		if _, exists := stateSet[name]; !exists {
			missingValues = append(missingValues, value.schema)
		}
	}

	if missingValues == nil {
		return nil
	}

	return &messages.FromFunction{
		Response: &messages.FromFunction_IncompleteInvocationContext_{
			IncompleteInvocationContext: &messages.FromFunction_IncompleteInvocationContext{
				MissingValues: missingValues,
			},
		},
	}
}

func fromFunction(states map[string]*persistedValue, out *output) (*messages.FromFunction, error) {
	var mutations []*messages.FromFunction_PersistedValueMutation
	for name, state := range states {
		if !state.updated {
			continue
		}

		var err error
		var bytes []byte
		var mutationType messages.FromFunction_PersistedValueMutation_MutationType

		if state.value == nil {
			bytes = nil
			mutationType = messages.FromFunction_PersistedValueMutation_DELETE
		} else {
			mutationType = messages.FromFunction_PersistedValueMutation_MODIFY

			bytes, err = proto.Marshal(state.value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize result for runtime")
			}
		}

		mutation := &messages.FromFunction_PersistedValueMutation{
			MutationType: mutationType,
			StateName:    name,
			StateValue:   bytes,
		}

		mutations = append(mutations, mutation)
	}

	return &messages.FromFunction{
		Response: &messages.FromFunction_InvocationResult{
			InvocationResult: &messages.FromFunction_InvocationResponse{
				StateMutations:     mutations,
				OutgoingMessages:   out.invocations,
				DelayedInvocations: out.delayedInvocation,
				OutgoingEgresses:   out.outgoingEgress,
			},
		},
	}, nil
}
