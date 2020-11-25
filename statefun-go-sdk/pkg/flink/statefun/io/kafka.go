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

package io

import (
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/errors"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/messages"
	"google.golang.org/protobuf/proto"
)

// Egress message that will be
// written out to Apache Kafka.
type KafkaRecord struct {
	// The topic to which the message
	// should be written.
	Topic string

	// An optional key to be written with
	// the message into the topic.
	Key string

	// The message to be written
	// to the topic.
	Value proto.Message
}

// Transforms a KafkaRecord into a Message that can
// be sent to an egress.
func (record *KafkaRecord) ToMessage() (proto.Message, error) {
	if record.Topic == "" {
		return nil, errors.New("cannot send a message to an empty topic")
	}

	marshalled, err := internal.Marshall(record.Value)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshall message meant for kafka topic %s", record.Topic)
	}

	var bytes []byte

	if marshalled != nil {
		bytes, err = proto.Marshal(marshalled)
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize message meant for kafka topic %s", record.Topic)
		}
	}

	return &messages.KafkaProducerRecord{
		Key:        record.Key,
		Topic:      record.Topic,
		ValueBytes: bytes,
	}, nil
}
