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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/messages"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
	"time"
)

const (
	// The name of the State object under which it will
	// be stored in the Stateful Functions runtime. This
	// tag is required and must be unique within the function.
	//
	// 	type MyFunction struct {
	//		MyState State `state:"my-state"`
	//	}
	StateNameTag = "state"

	// If set, a time-to-live will be configured for the state
	// value. If the function is not invoked for more than some
	// configured duration - parsable via time.ParseDuration -
	// then the field will be automatically deleted.
	//
	// 	type MyFunction struct {
	//		MyState State `state:"my-state" expireAfterInvoke:"1m"`
	//	}
	//
	// This tag is optional. If not set, the state
	// value will only be deleted when manually
	// cleared via State.Clear
	ExpireAfterInvoke = "expireAfterInvoke"

	// If set, a time-to-live will be configured for the state
	// value. If the state is not written to for more than some
	// configured duration - parsable via time.ParseDuration -
	// then the field will be automatically deleted.
	//
	// 	type MyFunction struct {
	//		MyState State `state:"my-state" expireAfterWrite:"1m"`
	//	}
	//
	// This tag is optional. If not set, the state
	// value will only be deleted when manually
	// cleared via State.Clear
	ExpireAfterWrite = "expireAfterWrite"
)

// State represents a value persisted by the runtime
// whose value is always scoped to the current Address.
//
// A StatefulFunction may contain any number of state
// instances. Each State requires a unique name tag
// under which it will be stored.
//
//	type MyFunction struct {
//		MyState State `state:"my-state"`
//	}
//
// The concrete implementation of this interface will be
// automatically provided by the runtime.
type State interface {
	// Get retrieves the state and unmarshalls the encoded value
	// contained into the provided message state.
	// It returns an error if the target message does not match the type
	// in the Any message or if an unmarshal error occurs.
	// The exists return value reports whether the state is currently
	// set so callers can differentiate missing values from the
	// empty type.
	Get(state proto.Message) (exists bool, err error)

	// Set stores the value and marshals the given message
	// value into an anypb.Any message if it is not already.
	Set(value proto.Message) error

	// Clear deletes the registered state. The value will
	// be fully deleted from the Stateful Function runtime
	// and subsequent calls to Get will return false
	// for the exists return value.
	Clear()
}

type persistedValue struct {
	value   *anypb.Any
	updated bool
	schema  *messages.FromFunction_PersistedValueSpec
}

func (p *persistedValue) Get(state proto.Message) (bool, error) {
	if p.value == nil || p.value.TypeUrl == "" {
		return false, nil
	}

	return true, internal.Unmarshall(p.value, state)
}

func (p *persistedValue) Set(value proto.Message) error {
	packedState, err := internal.Marshall(value)
	if err != nil {
		return err
	}

	p.updated = true
	p.value = packedState

	return nil
}

func (p *persistedValue) Clear() {
	p.updated = true
	p.value = nil
}

// reflectively iterates over the fields of the the implementation of the StatefulFunction
// interface, as passed to RegisterFunction, and sets its State fields as configured via
// its tags. Pointers to the implementation - persistedValue - are passed back so the
// runtime can set and check state values after each invocation. The concrete type of
// value must be a pointer to a struct because concrete structs do not have addressable
// fields.
func getStateFields(value interface{}) (map[string]*persistedValue, error) {
	val := reflect.ValueOf(value)

	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	} else {
		return nil, invalidTypeError(val)
	}

	// should double check we now have a struct (could still be anything)
	if val.Kind() != reflect.Struct {
		return nil, invalidTypeError(val)
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
			return nil, errors.New("duplicate states declared with state `name` %s", state.schema.StateName)
		}

		states[state.schema.StateName] = state
		val.Field(i).Set(reflect.ValueOf(state))
	}

	return states, nil
}

func invalidTypeError(val reflect.Value) error {
	return errors.New("statefun.StatefulFunction implementation must be a pointer to a struct, %v provided instead", val.Kind().String())
}

// generates the internal schema of a state based on the fields tags
func getSchema(field string, tags reflect.StructTag) (*messages.FromFunction_PersistedValueSpec, error) {
	name, ok := tags.Lookup(StateNameTag)
	if !ok {
		return nil, errors.New("field %s is missing required tag `%s`", field, StateNameTag)
	}

	spec := &messages.FromFunction_PersistedValueSpec{
		StateName: name,
	}

	afterInvokeDuration, expireAfterInvoke := tags.Lookup(ExpireAfterInvoke)
	afterWriteDuration, expireAfterWrite := tags.Lookup(ExpireAfterWrite)

	if expireAfterInvoke && expireAfterWrite {
		return nil, errors.New("invalid configuration for field %s, state values can only contain at most 1 expiration tag", field)
	}

	if expireAfterInvoke {
		duration, err := time.ParseDuration(afterInvokeDuration)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse duration for state field %s", field)
		}

		spec.ExpirationSpec = &messages.FromFunction_ExpirationSpec{
			Mode:              messages.FromFunction_ExpirationSpec_AFTER_INVOKE,
			ExpireAfterMillis: duration.Milliseconds(),
		}
	}

	if expireAfterWrite {
		duration, err := time.ParseDuration(afterWriteDuration)
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
