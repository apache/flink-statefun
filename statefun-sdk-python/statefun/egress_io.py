################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import struct
import typing

from statefun.core import Type
from statefun.messages import EgressMessage

from statefun.kafka_egress_pb2 import KafkaProducerRecord
from statefun.kinesis_egress_pb2 import KinesisEgressRecord
from statefun.request_reply_pb2 import TypedValue


def kafka_egress_message(typename: str,
                         topic: str,
                         value: typing.Union[str, bytes, bytearray, int, float],
                         value_type: Type = None,
                         key: str = None):
    """
    Build a message that can be emitted to a Kafka generic egress.

    If a value_type is provided, then @value will be serialized according to the
    provided value_type's serializer. Otherwise we will try to convert @value to bytes
    if it is one of:
    - utf-8 string
    - bytes
    - bytearray
    - an int (as defined by Kafka's serialization format)
    - float (as defined by Kafka's serialization format)

    :param typename: the target egress to emit to (as defined in the module.yaml)
    :param topic: The Kafka destination topic for that record
    :param key: the utf8 encoded string key to produce (can be empty)
    :param value: the value to produce
    :param value_type: an optional hint to this value type.
    :return: A Protobuf message representing the record to be produced via the Kafka generic egress.
    """
    if not topic:
        raise ValueError("A destination Kafka topic is missing")
    if value is None:
        raise ValueError("Missing value")
    record = KafkaProducerRecord()
    record.topic = topic
    if value_type:
        ser = value_type.serializer()
        record.value_bytes = ser.serialize(value)
    elif isinstance(value, str):
        record.value_bytes = bytes(value, 'utf-8')
    elif isinstance(value, (bytes, bytearray)):
        record.value_bytes = bytes(value)
    elif isinstance(value, int):
        # see:
        # IntegerSerializer Javadoc
        # https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/IntegerSerializer.html
        record.value_bytes = struct.pack('>i', value)
    elif isinstance(value, float):
        # see:
        # DoubleDeserializer Javadoc
        # https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/DoubleDeserializer.html
        record.value_bytes = struct.pack('>d', value)
    else:
        raise TypeError("Unable to convert value to bytes.")
    if key is not None:
        record.key = key

    typed_value = TypedValue()
    typed_value.typename = "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord"
    typed_value.has_value = True
    typed_value.value = record.SerializeToString()

    return EgressMessage(typename, typed_value)


def kinesis_egress_message(typename: str,
                           stream: str,
                           value: typing.Union[str, bytes, bytearray],
                           partition_key: str,
                           value_type: typing.Union[None, Type] = None,
                           explicit_hash_key: str = None):
    """
    Build a message that can be emitted to a Kinesis generic egress.

    :param typename: the typename as specified in module.yaml
    :param stream: The AWS Kinesis destination stream for that record
    :param partition_key: the utf8 encoded string partition key to use
    :param value: the value to produce
    :param explicit_hash_key: a utf8 encoded string explicit hash key to use (can be empty)
    :param value_type: an optional hint to this value type
    :return: A Protobuf message representing the record to be produced to AWS Kinesis via the Kinesis generic egress.
    """
    if not stream:
        raise ValueError("Missing destination Kinesis stream")
    if value is None:
        raise ValueError("Missing value")
    if partition_key is None:
        raise ValueError("Missing partition key")
    record = KinesisEgressRecord()
    record.stream = stream
    if value_type:
        ser = value_type.serializer()
        record.value_bytes = ser.serialize(value)
    elif isinstance(value, str):
        record.value_bytes = bytes(value, 'utf-8')
    elif isinstance(value, (bytes, bytearray)):
        record.value_bytes = bytes(value)
    else:
        raise TypeError("Unable to convert value to bytes.")
    record.partition_key = partition_key
    if explicit_hash_key is not None:
        record.explicit_hash_key = explicit_hash_key

    typed_value = TypedValue()
    typed_value.typename = "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord"
    typed_value.has_value = True
    typed_value.value = record.SerializeToString()

    return EgressMessage(typename, typed_value)
