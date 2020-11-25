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
//	  import "net/http"
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
//	  import "github.com/aws/aws-lambda"
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
	module map[FunctionType]*invokableFunction
}

func NewFunctionRegistry() FunctionRegistry {
	return &functions{
		module: make(map[FunctionType]*invokableFunction),
	}
}

func (functions *functions) RegisterFunction(funcType FunctionType, function StatefulFunction) error {
	states, err := getStateFields(function)
	if err != nil {
		return errors.Wrap(err, "failed to register function %s", funcType.String())
	}

	log.Printf("registering stateful function %s with states %s", funcType.String(), stateSchemaStringer(states))

	functions.module[funcType] = &invokableFunction{
		function: function,
		states:   states,
	}

	return nil
}

func (functions functions) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := &messages.ToFunction{}
	if err := proto.Unmarshal(payload, toFunction); err != nil {
		return nil, errors.BadRequest("failed to unmarshal payload %w", err)
	}

	target := toFunction.GetInvocation().Target

	funcType := FunctionType{
		Namespace: target.Namespace,
		Type:      target.Type,
	}

	invocable, exists := functions.module[funcType]
	if !exists {
		return nil, errors.BadRequest("%s does not exist", funcType.String())
	}

	fromFunction, err := invocable.invoke(ctx, toFunction)
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

func stateSchemaStringer(states map[string]*persistedValue) []string {
	var schemas []string
	for _, value := range states {
		schemas = append(schemas, value.schema.String())
	}

	return schemas
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
