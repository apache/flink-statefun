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
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/messages"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
)

// Keeps a mapping from FunctionType to stateful functions
// and serves them to the Flink runtime.
//
// HTTP Endpoint
//
//    import "net/http"
//
//	  func main() {
//    	registry := NewFunctionRegistry()
//		registry.RegisterFunction(greeterType, GreeterFunction{})
//
//	  	http.Handle("/service", registry)
//	  	_ = http.ListenAndServe(":8000", nil)
//	  }
//
// AWS Lambda
//
//    import "github.com/aws/aws-lambda"
//
//	  func main() {
//    	registry := NewFunctionRegistry()
//		registry.RegisterFunction(greeterType, GreeterFunction{})
//
//		lambda.StartHandler(registry)
//	  }
type FunctionRegistry interface {
	// Handler for processing runtime messages from
	// an http endpoint
	http.Handler

	// Handler for processing arbitrary payloads.
	// This method provides compliance with AWS Lambda
	// handler.
	Invoke(ctx context.Context, payload []byte) ([]byte, error)

	// Register a StatefulFunction under a FunctionType.
	RegisterFunction(funcType FunctionType, function StatefulFunction) error
}

type functions struct {
	module map[FunctionType]StatefulFunction
	states map[FunctionType]map[string]*persistedValue
}

func NewFunctionRegistry() FunctionRegistry {
	return &functions{
		module: make(map[FunctionType]StatefulFunction),
		states: make(map[FunctionType]map[string]*persistedValue),
	}
}

func (functions *functions) RegisterFunction(funcType FunctionType, function StatefulFunction) error {
	log.Printf("registering stateful function %s", funcType.String())

	states, err := getStateFields(function)
	if err != nil {
		return err
	}

	functions.module[funcType] = function
	functions.states[funcType] = states

	return nil
}

func (functions functions) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := &messages.ToFunction{}
	if err := proto.Unmarshal(payload, toFunction); err != nil {
		return nil, errors.BadRequest("failed to unmarshal payload %w", err)
	}

	fromFunction, err := functions.executeBatch(ctx, toFunction)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(fromFunction)
}

func (functions functions) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !validRequest(w, req) {
		return
	}

	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

	_, err := buffer.ReadFrom(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bytes, err := functions.Invoke(req.Context(), buffer.Bytes())
	if err != nil {
		http.Error(w, err.Error(), errors.ToCode(err))
		log.Print(err)
		return
	}

	_, _ = w.Write(bytes)
}

// perform basic HTTP validation on the incoming payload
func validRequest(w http.ResponseWriter, req *http.Request) bool {
	if req.Method != "POST" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return false
	}

	contentType := req.Header.Get("Content-type")
	if contentType != "" && contentType != "application/octet-stream" {
		http.Error(w, "invalid content type", http.StatusUnsupportedMediaType)
		return false
	}

	if req.Body == nil || req.ContentLength == 0 {
		http.Error(w, "empty request body", http.StatusBadRequest)
		return false
	}

	return true
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

func (functions functions) executeBatch(ctx context.Context, request *messages.ToFunction) (*messages.FromFunction, error) {
	invocations := request.GetInvocation()
	if invocations == nil {
		return nil, errors.BadRequest("missing invocations for batch")
	}

	funcType := FunctionType{
		Namespace: invocations.Target.Namespace,
		Type:      invocations.Target.Type,
	}

	function, exists := functions.module[funcType]
	if !exists {
		return nil, errors.BadRequest("%s does not exist", funcType.String())
	}

	if missing := setRegisteredStates(request.GetInvocation().State, functions.states[funcType]); missing != nil {
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
			err := function.Invoke(ctx, out, (*invocation).Argument)

			if err != nil {
				return nil, errors.Wrap(err, "failed to execute function %s", self.String())
			}
		}
	}

	return fromFunction(functions.states[funcType], out)
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
