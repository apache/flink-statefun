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

package internal

import (
	"bytes"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestFastBoolSerializer(t *testing.T) {
	test := func(n bool) {
		roundTrip(t, n, func(i interface{}) []byte {
			buffer := &bytes.Buffer{}
			_ = FastBoolSerializer(buffer, i.(bool))
			return buffer.Bytes()
		}, func(i []byte) interface{} {
			wrapper := protocol.BooleanWrapper{}
			_ = proto.Unmarshal(i, &wrapper)
			return wrapper.Value
		})
	}

	test(true)
	test(false)
}

func TestFastBoolDeserializer(t *testing.T) {
	test := func(n bool) {
		roundTrip(t, n, func(i interface{}) []byte {
			wrapper := protocol.BooleanWrapper{}
			wrapper.Value = i.(bool)
			data, _ := proto.Marshal(&wrapper)
			return data
		}, func(i []byte) interface{} {
			n, _ := FastBoolDeserializer(bytes.NewReader(i))
			return n
		})
	}

	test(true)
	test(false)
}

func TestFastInt32Serializer(t *testing.T) {
	test := func(n int32) {
		roundTrip(t, n, func(i interface{}) []byte {
			buffer := &bytes.Buffer{}
			_ = FastInt32Serializer(buffer, i.(int32))
			return buffer.Bytes()
		}, func(i []byte) interface{} {
			wrapper := protocol.IntWrapper{}
			_ = proto.Unmarshal(i, &wrapper)
			return wrapper.Value
		})
	}

	for i := -1_000_000; i < 1_000_000; i++ {
		test(int32(i))
	}
}

func TestFastInt32Deserializer(t *testing.T) {
	test := func(n int32) {
		roundTrip(t, n, func(i interface{}) []byte {
			wrapper := protocol.IntWrapper{}
			wrapper.Value = i.(int32)
			data, _ := proto.Marshal(&wrapper)
			return data
		}, func(i []byte) interface{} {
			n, _ := FastInt32Deserializer(bytes.NewReader(i))
			return n
		})
	}

	for i := -1_000_000; i < 1_000_000; i++ {
		test(int32(i))
	}
}

func TestFastInt64Serializer(t *testing.T) {
	test := func(n int64) {
		roundTrip(t, n, func(i interface{}) []byte {
			buffer := &bytes.Buffer{}
			_ = FastInt64Serializer(buffer, i.(int64))
			return buffer.Bytes()
		}, func(i []byte) interface{} {
			wrapper := protocol.LongWrapper{}
			_ = proto.Unmarshal(i, &wrapper)
			return wrapper.Value
		})
	}

	for i := -1_000_000; i < 1_000_000; i++ {
		test(int64(i))
	}
}

func TestFastInt64Deserializer(t *testing.T) {
	test := func(n int64) {
		roundTrip(t, n, func(i interface{}) []byte {
			wrapper := protocol.LongWrapper{}
			wrapper.Value = i.(int64)
			data, _ := proto.Marshal(&wrapper)
			return data
		}, func(i []byte) interface{} {
			n, _ := FastInt64Deserializer(bytes.NewReader(i))
			return n
		})
	}

	for i := -1_000_000; i < 1_000_000; i++ {
		test(int64(i))
	}
}

func TestFastStringSerializer(t *testing.T) {
	test := func(n string) {
		roundTrip(t, n, func(i interface{}) []byte {
			buffer := &bytes.Buffer{}
			_ = FastStringSerializer(buffer, i.(string))
			return buffer.Bytes()
		}, func(i []byte) interface{} {
			wrapper := protocol.StringWrapper{}
			_ = proto.Unmarshal(i, &wrapper)
			return wrapper.Value
		})
	}

	test("")
	test("this is a string")
}

func TestFastStringDeserializer(t *testing.T) {
	test := func(n string) {
		roundTrip(t, n, func(i interface{}) []byte {
			wrapper := protocol.StringWrapper{}
			wrapper.Value = i.(string)
			data, _ := proto.Marshal(&wrapper)
			return data
		}, func(i []byte) interface{} {
			n, _ := FastStringDeserializer(bytes.NewReader(i))
			return n
		})
	}

	test("")
	test("this is a string")
}

func roundTrip(t *testing.T, original interface{}, serialize func(interface{}) []byte, deserialize func([]byte) interface{}) {
	binary := serialize(original)
	result := deserialize(binary)

	assert.Equal(t, original, result)
}
