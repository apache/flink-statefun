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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoolType(t *testing.T) {
	testBool(t, true)
	testBool(t, false)
}

func testBool(t *testing.T, data bool) {
	buffer := bytes.Buffer{}
	err := BoolType.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result bool
	err = BoolType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

func TestIntType(t *testing.T) {
	testInt32(t, 1)
	testInt32(t, 1048576)
	testInt32(t, math.MaxInt32)
	testInt32(t, math.MinInt32)
	testInt32(t, -1)
}

func testInt32(t *testing.T, data int32) {
	buffer := bytes.Buffer{}
	err := Int32Type.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result int32
	err = Int32Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

func TestLongType(t *testing.T) {
	testInt64(t, -1)
	testInt64(t, 0)
	testInt64(t, math.MinInt64)
	testInt64(t, math.MaxInt64)
}

func testInt64(t *testing.T, data int64) {
	buffer := bytes.Buffer{}
	err := Int64Type.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result int64
	err = Int64Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

func TestFloatType(t *testing.T) {
	testFloat32(t, math.MaxFloat32)
	testFloat32(t, math.SmallestNonzeroFloat32)
	testFloat32(t, 2.1459)
	testFloat32(t, -1e4)
}

func testFloat32(t *testing.T, data float32) {
	buffer := bytes.Buffer{}
	err := Float32Type.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result float32
	err = Float32Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

func TestDoubleType(t *testing.T) {
	testFloat64(t, math.MaxFloat64)
	testFloat64(t, math.SmallestNonzeroFloat64)
	testFloat64(t, 2.1459)
	testFloat64(t, -1e4)
}

func testFloat64(t *testing.T, data float64) {
	buffer := bytes.Buffer{}
	err := Float64Type.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result float64
	err = Float64Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

func TestStringType(t *testing.T) {
	testString(t, "")
	testString(t, "This is a string")
}

func testString(t *testing.T, data string) {
	buffer := bytes.Buffer{}
	err := StringType.Serialize(&buffer, data)
	assert.NoError(t, err)

	var result string
	err = StringType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, data, result)
}

type User struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func TestJsonType(t *testing.T) {
	buffer := bytes.Buffer{}
	userType := MakeJsonType(MustParseTypeName("org.foo.bar/UserJson"))

	err := userType.Serialize(&buffer, User{"bob", "mop"})
	assert.NoError(t, err)

	var result User
	err = userType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, User{"bob", "mop"})
}
