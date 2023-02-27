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
	"encoding/json"
	"errors"
	"io"
	"log"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// SimpleType interface is the core abstraction used by Stateful
// Function's type system, and consists of a few things
// that StateFun uses to handle Message's and ValueSpec's
//
// 1. TypeName to identify the type.
// 2. (De)serialization methods for marshalling and unmarshalling data
//
// # Cross-language primitive types
//
// StateFun's type system has cross-language support for common primitive
// types, such as boolean, integer (int32), long (int64), etc. These
// primitive types have built-in SimpleType's implemented for them already
// with predefined TypeName's.
//
// These primitives have standard encoding across all StateFun language
// SDKs, so functions in various other languages (Java, Python, etc) can
// message Golang functions by directly sending supported primitive
// values as message arguments. Moreover, the type system is used for
// state values as well; so you can expect that a function can safely
// read previous state after reimplementing it in a different language.
//
// # Common custom types
//
// The type system is also very easily extensible to support more complex types.
// The Go SDK ships with predefined support for JSON and Protobuf - see MakeJsonType
// MakeProtobufType. For other formats, it is just a matter of implementing
// your own SimpleType with a custom typename and serializer.
type SimpleType interface {
	GetTypeName() TypeName

	Deserialize(r io.Reader, receiver interface{}) error

	Serialize(writer io.Writer, data interface{}) error
}

type PrimitiveType int

const (
	BoolType PrimitiveType = iota
	Int32Type
	Int64Type
	Float32Type
	Float64Type
	StringType
)

var (
	boolWrapperType    = MakeProtobufTypeWithTypeName(boolTypeName)
	int32WrapperType   = MakeProtobufTypeWithTypeName(int32TypeName)
	int64WrapperType   = MakeProtobufTypeWithTypeName(int64TypeName)
	float32WrapperType = MakeProtobufTypeWithTypeName(float64TypeName)
	float64WrapperType = MakeProtobufTypeWithTypeName(float64TypeName)
	stringWrapperType  = MakeProtobufTypeWithTypeName(stringTypeName)
)

func (p PrimitiveType) GetTypeName() TypeName {
	switch p {
	case BoolType:
		return boolTypeName
	case Int32Type:
		return int32TypeName
	case Int64Type:
		return int64TypeName
	case Float32Type:
		return float32TypeName
	case Float64Type:
		return float64TypeName
	case StringType:
		return stringTypeName
	default:
		// unreachable
		return nil
	}
}

func (p PrimitiveType) Deserialize(r io.Reader, receiver interface{}) error {
	switch p {
	case BoolType:
		switch data := receiver.(type) {
		case *bool:
			var wrapper protocol.BooleanWrapper
			if err := boolWrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}

			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type bool or *bool")
		}
	case Int32Type:
		switch data := receiver.(type) {
		case *int32:
			var wrapper protocol.IntWrapper
			if err := int32WrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}

			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type *int32")
		}
	case Int64Type:
		switch data := receiver.(type) {
		case *int64:
			var wrapper protocol.LongWrapper
			if err := int64WrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}
			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type *int64")
		}
	case Float32Type:
		switch data := receiver.(type) {
		case *float32:
			var wrapper protocol.FloatWrapper
			if err := float32WrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}

			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type *float32")
		}
	case Float64Type:
		switch data := receiver.(type) {
		case *float64:
			var wrapper protocol.DoubleWrapper
			if err := float64WrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}

			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type *float64")
		}
	case StringType:
		switch data := receiver.(type) {
		case *string:
			var wrapper protocol.StringWrapper
			if err := stringWrapperType.Deserialize(r, &wrapper); err != nil {
				return err
			}

			*data = wrapper.Value
		default:
			return errors.New("receiver must be of type *string")
		}
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil
	}

	return nil
}

func (p PrimitiveType) Serialize(writer io.Writer, data interface{}) error {
	switch p {
	case BoolType:
		switch data := data.(type) {
		case bool:
			wrapper := protocol.BooleanWrapper{Value: data}
			return boolWrapperType.Serialize(writer, &wrapper)
		case *bool:
			wrapper := protocol.BooleanWrapper{Value: *data}
			return boolWrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type bool or *bool")
		}
	case Int32Type:
		switch data := data.(type) {
		case int32:
			wrapper := protocol.IntWrapper{Value: data}
			return int32WrapperType.Serialize(writer, &wrapper)
		case *int32:
			wrapper := protocol.IntWrapper{Value: *data}
			return int32WrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type int32 or *int32")
		}
	case Int64Type:
		switch data := data.(type) {
		case int64:
			wrapper := protocol.LongWrapper{Value: data}
			return int64WrapperType.Serialize(writer, &wrapper)
		case *int64:
			wrapper := protocol.LongWrapper{Value: *data}
			return int64WrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type int64 or *int64")
		}
	case Float32Type:
		switch data := data.(type) {
		case float32:
			wrapper := protocol.FloatWrapper{Value: data}
			return float32WrapperType.Serialize(writer, &wrapper)
		case *float32:
			wrapper := protocol.FloatWrapper{Value: *data}
			return float32WrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type float32 or *float32")
		}
	case Float64Type:
		switch data := data.(type) {
		case float64:
			wrapper := protocol.DoubleWrapper{Value: data}
			return float64WrapperType.Serialize(writer, &wrapper)
		case *float64:
			wrapper := protocol.DoubleWrapper{Value: *data}
			return float64WrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type float64 or *float64")
		}
	case StringType:
		switch data := data.(type) {
		case string:
			wrapper := protocol.StringWrapper{Value: data}
			return stringWrapperType.Serialize(writer, &wrapper)
		case *string:
			wrapper := protocol.StringWrapper{Value: *data}
			return stringWrapperType.Serialize(writer, &wrapper)
		default:
			return errors.New("data must be of type string or *string")
		}
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil
	}
}

type jsonType struct {
	typeName TypeName
}

// MakeJsonType creates  a new SimpleType with a given TypeName
// using the standard Go JSON library.
func MakeJsonType(name TypeName) SimpleType {
	return jsonType{typeName: name}
}

func (j jsonType) GetTypeName() TypeName {
	return j.typeName
}

func (j jsonType) Deserialize(r io.Reader, receiver interface{}) error {
	return json.NewDecoder(r).Decode(receiver)
}

func (j jsonType) Serialize(writer io.Writer, data interface{}) error {
	return json.NewEncoder(writer).Encode(data)
}

type protoType struct {
	typeName TypeName
}

// MakeProtobufType creates a new SimpleType for the given protobuf Message.
func MakeProtobufType(m proto.Message) SimpleType {
	name := proto.MessageName(m)
	tName, _ := TypeNameFromParts("type.googleapis.com", string(name))
	return MakeProtobufTypeWithTypeName(tName)
}

// MakeProtobufTypeWithTypeName creates a new SimpleType for the
// given protobuf Message with a custom namespace.
func MakeProtobufTypeWithTypeName(typeName TypeName) SimpleType {
	return protoType{
		typeName: typeName,
	}
}

func (p protoType) GetTypeName() TypeName {
	return p.typeName
}

func (p protoType) Deserialize(r io.Reader, receiver interface{}) (err error) {
	switch receiver := receiver.(type) {
	case proto.Message:
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		return proto.Unmarshal(data, receiver)
	default:
		return errors.New("receiver must implement proto.Message")
	}
}

func (p protoType) Serialize(writer io.Writer, data interface{}) error {
	switch data := data.(type) {
	case proto.Message:
		if value, err := proto.Marshal(data); err != nil {
			return err
		} else {
			_, err = writer.Write(value)
			return err
		}

	default:
		return errors.New("data must implement proto.Message")
	}
}
