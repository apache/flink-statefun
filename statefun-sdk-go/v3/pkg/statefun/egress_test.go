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

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestKafkaEgressBuilder(t *testing.T) {
	k := KafkaEgressBuilder{
		Target: MustParseTypeName("example/target"),
		Topic:  "topic",
		Key:    "key",
		Value:  "value",
	}

	msg, err := k.toEgressMessage()
	assert.NoError(t, err, "failed to build Kafka egress message")

	var result protocol.KafkaProducerRecord
	err = proto.Unmarshal(msg.Argument.Value, &result)

	assert.NoError(t, err, "failed to deserialize Kafka producer record")
	assert.Equal(t, "key", result.Key)
	assert.Equal(t, "value", string(result.ValueBytes))
	assert.Equal(t, "topic", result.Topic)
}

func TestKafkaEgressBuilderInvalidString(t *testing.T) {
	k := KafkaEgressBuilder{
		Target: MustParseTypeName("example/target"),
		Topic:  "topic",
		Key:    "key",
		Value:  string([]byte{0xff, 0xfe, 0xfd}),
	}

	_, err := k.toEgressMessage()
	assert.Errorf(t, err, "built Kafka egress message with invalid string")
}

func TestKinesisEgressBuilder(t *testing.T) {
	k := KinesisEgressBuilder{
		Target:       MustParseTypeName("example/target"),
		Stream:       "stream",
		PartitionKey: "key",
		Value:        "value",
	}

	msg, err := k.toEgressMessage()
	assert.NoError(t, err, "failed to build Kinesis egress message")

	var result protocol.KinesisEgressRecord
	err = proto.Unmarshal(msg.Argument.Value, &result)

	assert.NoError(t, err, "failed to deserialize Kinesis producer record")
	assert.Equal(t, "stream", result.Stream)
	assert.Equal(t, "key", result.PartitionKey)
	assert.Equal(t, "value", string(result.ValueBytes))
}

func TestKinesisEgressBuilderInvalidString(t *testing.T) {
	k := KinesisEgressBuilder{
		Target:       MustParseTypeName("example/target"),
		Stream:       "stream",
		PartitionKey: "key",
		Value:        string([]byte{0xff, 0xfe, 0xfd}),
	}

	_, err := k.toEgressMessage()
	assert.Errorf(t, err, "built Kinesis egress message with invalid string")
}
