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
	"bytes"
	"context"
	"fmt"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
)

// StatefulFunctions is a registry for multiple StatefulFunction's. A RequestReplyHandler
// can be created from the registry that understands how to dispatch
// invocation requests to the registered functions as well as encode
// side-effects (e.g., sending messages to other functions or updating
// values in storage) as the response.
type StatefulFunctions interface {

	// WithSpec registers a StatefulFunctionSpec, which will be
	// used to build the runtime function. It returns an error
	// if the specification is invalid and the handler
	// fails to register the function.
	WithSpec(spec StatefulFunctionSpec) error

	// AsHandler creates a RequestReplyHandler from the registered
	// function specs.
	AsHandler() RequestReplyHandler
}

// The RequestReplyHandler processes messages
// from the runtime, invokes functions, and encodes
// side effects. The handler implements http.Handler
// so it can easily be embedded in standard Go server
// frameworks.
type RequestReplyHandler interface {
	http.Handler

	// Invoke method provides compliance with AWS Lambda handler
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
}

// StatefulFunctionsBuilder creates a new StatefulFunctions registry.
func StatefulFunctionsBuilder() StatefulFunctions {
	return &handler{
		module:     map[TypeName]StatefulFunction{},
		stateSpecs: map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec{},
	}
}

type handler struct {
	module     map[TypeName]StatefulFunction
	stateSpecs map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec
}

func (h *handler) WithSpec(spec StatefulFunctionSpec) error {
	if _, exists := h.module[spec.FunctionType]; exists {
		err := fmt.Errorf("failed to register Stateful Function %s, there is already a spec registered under that tpe", spec.FunctionType)
		log.Printf(err.Error())
		return err
	}

	if spec.Function == nil {
		err := fmt.Errorf("failed to register Stateful Function %s, the Function instance cannot be nil", spec.FunctionType)
		log.Printf(err.Error())
		return err
	}

	valueSpecs := make(map[string]*protocol.FromFunction_PersistedValueSpec, len(spec.States))

	for _, state := range spec.States {
		if err := validateValueSpec(state); err != nil {
			err := fmt.Errorf("failed to register Stateful Function %s: %w", spec.FunctionType, err)
			log.Printf(err.Error())
			return err
		}

		expiration := &protocol.FromFunction_ExpirationSpec{}
		switch state.Expiration.expirationType {
		case none:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_NONE
		case expireAfterWrite:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_WRITE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		case expireAfterCall:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_INVOKE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		}

		valueSpecs[state.Name] = &protocol.FromFunction_PersistedValueSpec{
			StateName:      state.Name,
			ExpirationSpec: expiration,
			TypeTypename:   state.ValueType.GetTypeName().String(),
		}
	}

	h.module[spec.FunctionType] = spec.Function
	h.stateSpecs[spec.FunctionType] = valueSpecs

	return nil
}

func (h *handler) AsHandler() RequestReplyHandler {
	log.Println("Create RequestReplyHandler")
	for typeName := range h.module {
		log.Printf("> Registering %s\n", typeName)
	}
	return h
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		http.Error(writer, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	contentType := request.Header.Get("Content-type")
	if contentType != "" && contentType != "application/octet-stream" {
		http.Error(writer, "invalid content type", http.StatusUnsupportedMediaType)
		return
	}

	if request.Body == nil || request.ContentLength == 0 {
		http.Error(writer, "empty request body", http.StatusBadRequest)
		return
	}

	buffer := bytes.Buffer{}
	if _, err := buffer.ReadFrom(request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := h.Invoke(request.Context(), buffer.Bytes())
	if err != nil {
		log.Printf(err.Error())
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = writer.Write(response)
}

func (h *handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := protocol.ToFunction{}
	if err := proto.Unmarshal(payload, &toFunction); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ToFunction: %w", err)
	}

	fromFunction, err := h.invoke(ctx, &toFunction)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(fromFunction)
}

func (h *handler) invoke(ctx context.Context, toFunction *protocol.ToFunction) (from *protocol.FromFunction, err error) {
	batch := toFunction.GetInvocation()
	self := addressFromInternal(batch.Target)
	function, exists := h.module[self.FunctionType]

	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				err = fmt.Errorf("failed to execute invocation for %s: %w", batch.Target, r)
			default:
				log.Fatal(r)
			}
		}
	}()

	if !exists {
		return nil, fmt.Errorf("unknown function type %s", self.FunctionType)
	}

	storageFactory := newStorageFactory(batch, h.stateSpecs[self.FunctionType])

	if missing := storageFactory.getMissingSpecs(); missing != nil {
		log.Printf("missing state specs for function type %v", self)
		for _, spec := range missing {
			log.Printf("registering missing specs %v", spec)
		}
		return &protocol.FromFunction{
			Response: &protocol.FromFunction_IncompleteInvocationContext_{
				IncompleteInvocationContext: &protocol.FromFunction_IncompleteInvocationContext{
					MissingValues: missing,
				},
			},
		}, nil
	}

	storage := storageFactory.getStorage()
	response := &protocol.FromFunction_InvocationResponse{}

	for _, invocation := range batch.Invocations {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			sContext := statefunContext{
				self:     self,
				storage:  storage,
				response: response,
			}

			var cancel context.CancelFunc
			sContext.Context, cancel = context.WithCancel(ctx)

			var caller Address
			if invocation.Caller != nil {
				caller = addressFromInternal(invocation.Caller)
			}
			sContext.caller = &caller
			msg := Message{
				target:     batch.Target,
				typedValue: invocation.Argument,
			}
			err = function.Invoke(&sContext, msg)
			cancel()

			if err != nil {
				return
			}
		}
	}

	response.StateMutations = storage.getStateMutations()
	from = &protocol.FromFunction{
		Response: &protocol.FromFunction_InvocationResult{
			InvocationResult: response,
		},
	}

	return
}
