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
	"fmt"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/messages"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
	"time"
)

// State represents a value persisted by the runtime
// whose value is always scoped to the current Address.
//
// A StatefulFunction may contain any number of state
// instances. Each State requires a unique name tag
// under which it will be stored.
//
//	type MyFunction struct {
//		MyState State `name:"my-state"`
//	}
//
// Optionally, a State may have an automatic expiration.
// - expireAfterInvoke
// - expireAfterWrite
//
// If none is set, the State is only deleted when explicitly
// deleted via the Clear method. The value of the expiration
// tag must be a duration parsable by time.ParseDuration.
//
//	type MyFunction struct {
//		MyState State `name:"my-state" expireAfterInvoke:"1m"`
//	}
type State interface {
	// Get retrieves the state and unmarshalls the encoded value
	// contained into the provided message state.
	// It returns an error if the target message does not match the type
	// in the Any message or if an unmarshal error occurs.
	Get(state proto.Message) (bool, error)

	// Set stores the value and marshals the given message
	// m into an any.Any message if it is not already.
	Set(value proto.Message) error

	// Clear deletes the registered state.
	Clear()
}

type persistedValue struct {
	value   *anypb.Any
	updated bool
	schema  *messages.FromFunction_PersistedValueSpec
}

func (p persistedValue) Get(state proto.Message) (bool, error) {
	if p.value == nil || p.value.TypeUrl == "" {
		return false, nil
	}

	return true, internal.Unmarshall(p.value, state)
}

func (p persistedValue) Set(value proto.Message) error {
	packedState, err := internal.Marshall(value)
	if err != nil {
		return err
	}

	p.updated = true
	p.value = packedState

	return nil
}

func (p persistedValue) Clear() {
	p.updated = true
	p.value = nil
}

func getStateFields(value interface{}) (map[string]*persistedValue, error) {
	val := reflect.ValueOf(value)

	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	} else {
		return nil, fmt.Errorf("stateful function must be a pointer to a struct")
	}

	// should double check we now have a struct (could still be anything)
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("stateful function implementation must be a pointer to a struct, %v provided instead", val.Kind().String())
	}

	stateType := reflect.TypeOf((*State)(nil)).Elem()

	states := make(map[string]*persistedValue)
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		if !field.Type.Implements(stateType) {
			continue
		}

		schema, err := getSchema(field.Name, field.Tag)
		if err != nil {
			return nil, err
		}
		state := &persistedValue{
			schema: schema,
		}

		if _, exists := states[state.schema.StateName]; exists {
			return nil, fmt.Errorf("duplicate states decalred under name %s", state.schema.StateName)
		}

		states[state.schema.StateName] = state
		val.Field(i).Set(reflect.ValueOf(state))
	}

	return states, nil
}

func getSchema(field string, tags reflect.StructTag) (*messages.FromFunction_PersistedValueSpec, error) {
	name, ok := tags.Lookup("name")
	if !ok {
		return nil, fmt.Errorf("field %s is missing required tag name", field)
	}

	spec := &messages.FromFunction_PersistedValueSpec{
		StateName: name,
	}

	if ttl, ok := tags.Lookup("expireAfterInvoke"); ok {
		duration, err := time.ParseDuration(ttl)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse duration for state field %s", field)
		}

		spec.ExpirationSpec = &messages.FromFunction_ExpirationSpec{
			Mode:              messages.FromFunction_ExpirationSpec_AFTER_INVOKE,
			ExpireAfterMillis: duration.Milliseconds(),
		}
	}

	if ttl, ok := tags.Lookup("expireAfterWrite"); ok {
		duration, err := time.ParseDuration(ttl)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse duration for state field %s", field)
		}

		spec.ExpirationSpec = &messages.FromFunction_ExpirationSpec{
			Mode:              messages.FromFunction_ExpirationSpec_AFTER_WRITE,
			ExpireAfterMillis: duration.Milliseconds(),
		}
	}

	return spec, nil
}
