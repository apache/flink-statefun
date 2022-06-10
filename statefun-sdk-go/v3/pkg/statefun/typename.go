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
	"errors"
	"fmt"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"strings"
)

var (
	boolTypeName    = TypeNameFrom("io.statefun.types/bool")
	int32TypeName   = TypeNameFrom("io.statefun.types/int")
	int64TypeName   = TypeNameFrom("io.statefun.types/long")
	float32TypeName = TypeNameFrom("io.statefun.types/float")
	float64TypeName = TypeNameFrom("io.statefun.types/double")
	stringTypeName  = TypeNameFrom("io.statefun.types/string")
)

// A TypeName is used to uniquely identify objects within
// a Stateful Functions application, including functions,
// egresses, and types. TypeName's serve as an integral
// part of identifying these objects for message delivery
// as well as message data serialization and deserialization.
type TypeName interface {
	fmt.Stringer
	GetNamespace() string
	GetType() string
}

type typeName struct {
	namespace      string
	tpe            string
	typenameString string
}

func (t typeName) String() string {
	return t.typenameString
}

func (t typeName) GetNamespace() string {
	return t.namespace
}

func (t typeName) GetType() string {
	return t.tpe
}

// TypeNameFrom creates a TypeName from a canonical string
// in the format `<namespace>/<type>`. This Function
// assumes correctly formatted strings and will panic
// on error. For runtime error handling please
// see ParseTypeName.
func TypeNameFrom(typename string) TypeName {
	result, err := ParseTypeName(typename)
	if err != nil {
		panic(err)
	}

	return result
}

// ParseTypeName creates a TypeName from a canonical string
// in the format `<namespace>/<type>`.
func ParseTypeName(typename string) (TypeName, error) {
	position := strings.LastIndex(typename, "/")
	if position <= 0 || position == len(typename)-1 {
		return nil, fmt.Errorf("%v does not conform to the <namespace>/<type> format", typename)
	}

	namespace := typename[:position]
	name := typename[position+1:]

	if namespace[len(namespace)-1] == '/' {
		namespace = namespace[:len(namespace)-1]
	}

	return TypeNameFromParts(namespace, name)
}

func TypeNameFromParts(namespace, tpe string) (TypeName, error) {
	if len(namespace) == 0 {
		return nil, errors.New("namespace cannot be empty")
	}

	if len(tpe) == 0 {
		return nil, errors.New("type cannot be empty")
	}

	return typeName{
		namespace:      namespace,
		tpe:            tpe,
		typenameString: fmt.Sprintf("%s/%s", namespace, tpe),
	}, nil
}

// An Address is the unique identity of an individual StatefulFunction,
// containing the function's FunctionType and a unique identifier
// within the type. The function's type denotes the type (or class) of function
// to invoke, while the unique identifier addresses the invocation to a specific
// function instance.
type Address struct {
	FunctionType TypeName
	Id           string
}

func (a Address) String() string {
	return fmt.Sprintf("Address(%s, %s, %s)", a.FunctionType.GetNamespace(), a.FunctionType.GetType(), a.Id)
}

func addressFromInternal(a *protocol.Address) Address {
	name, _ := TypeNameFromParts(a.Namespace, a.Type)
	return Address{
		FunctionType: name,
		Id:           a.Id,
	}
}
