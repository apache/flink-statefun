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
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestStatefunContext_Send(t *testing.T) {
	context := createContext()

	msg := MessageBuilder{
		Target: Address{
			FunctionType: TypeNameFrom("example/func"),
			Id:           "0",
		},
		Value: "hello",
	}

	context.Send(msg)
	outgoing := context.response.GetOutgoingMessages()

	assert.Equal(t, 1, len(outgoing), "incorrect number of outgoing messages")
	assert.Equal(t, "example", outgoing[0].Target.Namespace, "incorrect target namespace")
	assert.Equal(t, "func", outgoing[0].Target.Type, "incorrect target type")
	assert.Equal(t, "0", outgoing[0].Target.Id, "incorrect target id")
	assert.Equal(t, stringTypeName.String(), outgoing[0].Argument.Typename, "incorrect typename set for message")
	assert.True(t, outgoing[0].Argument.HasValue, "argument does not have value")
}

func TestStatefunContext_SendAfter(t *testing.T) {
	context := createContext()

	msg := MessageBuilder{
		Target: Address{
			FunctionType: TypeNameFrom("example/func"),
			Id:           "0",
		},
		Value: "hello",
	}

	context.SendAfter(time.Duration(1)*time.Millisecond, msg)
	delayed := context.response.GetDelayedInvocations()

	assert.Equal(t, 1, len(delayed), "incorrect number of delayed messages")
	assert.Equal(t, "example", delayed[0].Target.Namespace, "incorrect target namespace")
	assert.Equal(t, "func", delayed[0].Target.Type, "incorrect target type")
	assert.Equal(t, "0", delayed[0].Target.Id, "incorrect target id")
	assert.Equal(t, int64(1), delayed[0].DelayInMs, "incorrect delay")
	assert.Equal(t, "", delayed[0].CancellationToken, "set cancellation token")
	assert.False(t, delayed[0].IsCancellationRequest, "delayed message should not be a cancellation request")
	assert.Equal(t, stringTypeName.String(), delayed[0].Argument.Typename, "incorrect typename set for message")
	assert.True(t, delayed[0].Argument.HasValue, "argument does not have value")
}

func TestStatefunContext_SendAfterWithCancellationTokenMessage(t *testing.T) {
	context := createContext()

	msg := MessageBuilder{
		Target: Address{
			FunctionType: TypeNameFrom("example/func"),
			Id:           "0",
		},
		Value: "hello",
	}

	token, err := NewCancellationToken("token")
	assert.NoError(t, err, "failed to create token")

	context.SendAfterWithCancellationToken(time.Duration(1)*time.Millisecond, token, msg)
	delayed := context.response.GetDelayedInvocations()

	assert.Equal(t, 1, len(delayed), "incorrect number of delayed messages")
	assert.Equal(t, "example", delayed[0].Target.Namespace, "incorrect target namespace")
	assert.Equal(t, "func", delayed[0].Target.Type, "incorrect target type")
	assert.Equal(t, "0", delayed[0].Target.Id, "incorrect target id")
	assert.Equal(t, int64(1), delayed[0].DelayInMs, "incorrect delay")
	assert.Equal(t, token.Token(), delayed[0].CancellationToken, "failed to set cancellation token")
	assert.False(t, delayed[0].IsCancellationRequest, "delayed message should not be a cancellation request")
	assert.Equal(t, stringTypeName.String(), delayed[0].Argument.Typename, "incorrect typename set for message")
	assert.True(t, delayed[0].Argument.HasValue, "argument does not have value")
}

func TestStatefunContext_CancelDelayedMessage(t *testing.T) {
	context := createContext()
	token, err := NewCancellationToken("token")
	assert.NoError(t, err, "failed to create token")

	context.CancelDelayedMessage(token)
	delayed := context.response.GetDelayedInvocations()
	assert.Equal(t, 1, len(delayed), "incorrect number of delayed messages")
	assert.Equal(t, token.Token(), delayed[0].CancellationToken, "failed to set cancellation token")
	assert.True(t, delayed[0].IsCancellationRequest, "delayed message should be a cancellation request")
}

func TestStatefunContext_SendEgress_Kafka(t *testing.T) {
	context := createContext()

	kafka := &KafkaEgressBuilder{
		Target: TypeNameFrom("example/kafka"),
		Topic:  "topic",
		Key:    "key",
		Value:  "value",
	}

	context.SendEgress(kafka)
	egress := context.response.GetOutgoingEgresses()

	assert.Equal(t, 1, len(egress), "incorrect number of egress messages")
	assert.Equal(t, "example", egress[0].EgressNamespace, "incorrect target namespace")
	assert.Equal(t, "kafka", egress[0].EgressType, "incorrect target type")
	assert.Equal(t, kafkaTypeName, egress[0].Argument.Typename, "incorrect typename")

	kafkaRecord := protocol.KafkaProducerRecord{}
	assert.NoError(t, proto.Unmarshal(egress[0].Argument.Value, &kafkaRecord), "failed to deserialize kafka record")
	assert.Equal(t, "topic", kafkaRecord.Topic, "incorrect kafka topic")
	assert.Equal(t, "key", kafkaRecord.Key, "incorrect kafka key")
}

func TestStatefunContext_SendEgress_Kinesis(t *testing.T) {
	context := createContext()

	kafka := &KinesisEgressBuilder{
		Target:       TypeNameFrom("example/kinesis"),
		Stream:       "stream",
		PartitionKey: "key",
		Value:        "value",
	}

	context.SendEgress(kafka)
	egress := context.response.GetOutgoingEgresses()

	assert.Equal(t, 1, len(egress), "incorrect number of egress messages")
	assert.Equal(t, "example", egress[0].EgressNamespace, "incorrect target namespace")
	assert.Equal(t, "kinesis", egress[0].EgressType, "incorrect target type")
	assert.Equal(t, kinesisTypeName, egress[0].Argument.Typename, "incorrect typename")

	kinesis := protocol.KinesisEgressRecord{}
	assert.NoError(t, proto.Unmarshal(egress[0].Argument.Value, &kinesis), "failed to deserialize kinesis record")
	assert.Equal(t, "stream", kinesis.Stream, "incorrect kinesis stream")
	assert.Equal(t, "key", kinesis.PartitionKey, "incorrect kinesis key")
}

// creates a context with the minimal state to
// run tests.
func createContext() *statefunContext {
	return &statefunContext{
		response: &protocol.FromFunction_InvocationResponse{},
	}
}
