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
	"errors"
	"io"
	"math"
)

const (
	wireTypeVarInt          = 0
	wireTypeFixed32         = 5
	wireTypeFixed64         = 1
	wireTypeLengthDelimited = 2
)

var (
	boolTypeTag   = protobufTagAsSingleByte(1, wireTypeVarInt)
	int32TypeTag  = protobufTagAsSingleByte(1, wireTypeFixed32)
	int64TypeTag  = protobufTagAsSingleByte(1, wireTypeFixed64)
	stringTypeTag = protobufTagAsSingleByte(1, wireTypeLengthDelimited)
)

func FastBoolSerializer(w io.Writer, n bool) error {
	if n == false {
		return nil
	}

	buffer := [2]byte{
		boolTypeTag,
		1,
	}

	_, err := w.Write(buffer[:])
	return err
}

func FastBoolDeserializer(r io.Reader) (bool, error) {
	var buffer [2]byte
	if n, err := r.Read(buffer[:]); n == 0 {
		return false, nil
	} else if err != nil {
		return false, err
	}

	tag := buffer[0]
	value := buffer[1]

	if tag != boolTypeTag || value != byte(1) {
		return false, errors.New("not a Boolean value")
	}

	return true, nil
}

func FastInt32Serializer(w io.Writer, n int32) error {
	if n == 0 {
		return nil
	}

	out := [5]byte{
		int32TypeTag,
		byte(n & 0xFF),
		byte((n >> 8) & 0xFF),
		byte((n >> 16) & 0xFF),
		byte((n >> 24) & 0xFF),
	}

	_, err := w.Write(out[:])
	return err
}

func FastInt32Deserializer(r io.Reader) (int32, error) {
	var buffer [5]byte
	if n, err := r.Read(buffer[:]); n == 0 {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if buffer[0] != int32TypeTag {
		return 0, errors.New("not an IntWrapper")
	}

	n := int32(buffer[1] & 0xFF)
	n |= int32(buffer[2]&0xFF) << 8
	n |= int32(buffer[3]&0xFF) << 16
	n |= int32(buffer[4]&0xFF) << 24

	return n, nil
}

func FastInt64Serializer(w io.Writer, n int64) error {
	if n == 0 {
		return nil
	}

	out := [9]byte{
		int64TypeTag,
		byte(n & 0xFF),
		byte((n >> 8) & 0xFF),
		byte((n >> 16) & 0xFF),
		byte((n >> 24) & 0xFF),
		byte((n >> 32) & 0xFF),
		byte((n >> 40) & 0xFF),
		byte((n >> 48) & 0xFF),
		byte((n >> 56) & 0xFF),
	}

	_, err := w.Write(out[:])
	return err
}

func FastInt64Deserializer(r io.Reader) (int64, error) {
	var buffer [9]byte
	if n, err := r.Read(buffer[:]); n == 0 {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if buffer[0] != int64TypeTag {
		return 0, errors.New("not an LongWrapper")
	}

	n := int64(buffer[1] & 0xFF)
	n |= int64(buffer[2]&0xFF) << 8
	n |= int64(buffer[3]&0xFF) << 16
	n |= int64(buffer[4]&0xFF) << 24
	n |= int64(buffer[5]&0xFF) << 32
	n |= int64(buffer[6]&0xFF) << 40
	n |= int64(buffer[7]&0xFF) << 48
	n |= int64(buffer[8]&0xFF) << 56

	return n, nil
}

func FastStringSerializer(w io.Writer, n string) error {
	if len(n) == 0 {
		return nil
	}

	utf8 := []byte(n)
	header := make([]byte, 5+len(utf8))

	position := 1
	header[0] = stringTypeTag

	varIntLen := len(utf8)
	for (varIntLen & -128) != 0 {
		header[position] = byte(varIntLen&127 | 128)
		position++
		varIntLen >>= uint(7)
	}

	header[position] = byte(varIntLen)
	position++

	if _, err := w.Write(header[:position]); err != nil {
		return err
	}

	_, err := w.Write(utf8)
	return err
}

func FastStringDeserializer(r io.Reader) (string, error) {
	var tag [1]byte
	if n, err := r.Read(tag[:]); n == 0 {
		return "", nil
	} else if err != nil {
		return "", err
	}

	if tag[0] != stringTypeTag {
		return "", errors.New("not a StringWrapper")
	}

	position, shift, varIntSize := 0, 0, int64(0)
	buffer, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}

	for shift < 32 {
		b := buffer[position]
		position++
		varIntSize |= int64((b & 0x7F) << shift)
		if (b & 0x80) == 0 {
			break
		}

		shift += 7
	}

	// sanity checks
	if varIntSize < 0 || varIntSize > math.MaxInt32 {
		return "", errors.New("malformed VarInt")
	}

	endIndex := position + int(varIntSize)
	return string(buffer[position:endIndex]), nil
}

func protobufTagAsSingleByte(fieldNumber int, wireType int) byte {
	fieldTag := fieldNumber<<3 | wireType
	if fieldTag < -127 || fieldTag > 127 {
		panic("Protobuf Wrapper type compatibility is bigger than one byte")
	}

	return byte(fieldTag)
}
