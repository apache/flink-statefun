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

package internal

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"testing"
)

func TestUnmarshalNil(t *testing.T) {
	err := Unmarshall(&anypb.Any{}, nil)
	if err == nil {
		assert.Fail(t, "unmarshal should fail on nil receiver")
	}
}

func TestUnmarshalAny(t *testing.T) {
	value := anypb.Any{
		TypeUrl: "test/type",
		Value:   nil,
	}

	receiver := anypb.Any{}
	err := Unmarshall(&value, &receiver)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, &value, &receiver, "unmarshalling any into any should return itself")
}

func TestMarshalAny(t *testing.T) {
	value := &anypb.Any{
		TypeUrl: "test/type",
		Value:   nil,
	}

	marshalled, err := Marshall(value)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, value, marshalled, "marhsalled any should return itself")
}

func TestMarshalNil(t *testing.T) {
	marshalled, err := Marshall(nil)
	if err != nil {
		t.Error(err)
	}

	if marshalled != nil {
		assert.Fail(t, "marhsalled nil should be nil")
	}
}
