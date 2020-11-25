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
	"github.com/apache/flink-statefun/statefun-go-sdk/v2/pkg/flink/statefun/internal/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Marshall's the passed message to Any if it is not
// one already.
func Marshall(value proto.Message) (*anypb.Any, error) {
	var packedState *anypb.Any
	switch record := value.(type) {
	case nil:
		packedState = nil
	case *anypb.Any:
		packedState = record
	default:
		packedState = &anypb.Any{}
		if err := packedState.MarshalFrom(record); err != nil {
			return nil, errors.Wrap(err, "failed to marshall value into any")
		}
	}
	return packedState, nil
}

// Unmarshall's the provided record into the expected type.
func Unmarshall(value *anypb.Any, receiver proto.Message) error {
	switch unmarshalled := receiver.(type) {
	case nil:
		return errors.New("cannot unmarshall into nil receiver")
	case *anypb.Any:
		unmarshalled.TypeUrl = value.TypeUrl
		unmarshalled.Value = value.Value
	default:
		if err := value.UnmarshalTo(receiver); err != nil {
			return err
		}
	}

	return nil
}
