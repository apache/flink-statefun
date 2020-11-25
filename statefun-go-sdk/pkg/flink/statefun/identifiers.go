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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/messages"
)

// A reference to a stateful function, consisting of a namespace and a name.
// A function's type is part of a function's Address and serves as integral
// part of an individual function's identity.
type FunctionType struct {
	Namespace string
	Type      string
}

func (functionType *FunctionType) String() string {
	return fmt.Sprintf("%s/%s", functionType.Namespace, functionType.Type)
}

// An Address is the unique identity of an individual {@link StatefulFunction}, containing
// of the function's FunctionType and an unique identifier within the type. The function's
// type denotes the class of function to invoke, while the unique identifier addresses the
// invocation to a specific function instance.
type Address struct {
	FunctionType FunctionType
	Id           string
}

func (address *Address) String() string {
	return fmt.Sprintf("%s/%s", address.FunctionType.String(), address.Id)
}

func addressFromInternal(address *messages.Address) *Address {
	if address == nil {
		return nil
	}

	return &Address{
		FunctionType: FunctionType{
			Namespace: address.Namespace,
			Type:      address.Type,
		},
		Id: address.Id,
	}
}
