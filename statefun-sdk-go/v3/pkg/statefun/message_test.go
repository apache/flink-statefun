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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicIntMessage(t *testing.T) {
	typename, err := ParseTypeName("foo/bar")
	assert.NoError(t, err)

	message, err := MessageBuilder{
		Target: Address{
			FunctionType: typename,
			Id:           "a",
		},
		Value: int32(1),
	}.ToMessage()

	assert.NoError(t, err)
	assert.True(t, message.IsInt32())

	value, _ := message.AsInt32()
	assert.Equal(t, value, int32(1))
}

func TestMessageWithType(t *testing.T) {
	typename, err := ParseTypeName("foo/bar")
	assert.NoError(t, err)

	message, err := MessageBuilder{
		Target: Address{
			FunctionType: typename,
			Id:           "a",
		},
		Value:     float32(5.0),
		ValueType: Float32Type,
	}.ToMessage()

	assert.NoError(t, err)
	assert.True(t, message.IsFloat32())

	value, _ := message.AsFloat32()
	assert.Equal(t, value, float32(5.0))
}
