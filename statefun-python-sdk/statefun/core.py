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

from google.protobuf.any_pb2 import Any
import inspect

from enum import Enum
from typing import List
from datetime import timedelta

from statefun.kafka_egress_pb2 import KafkaProducerRecord
from statefun.kinesis_egress_pb2 import KinesisEgressRecord

class SdkAddress(object):
    def __init__(self, namespace, type, identity):
        self.namespace = namespace
        self.type = type
        self.identity = identity

    def __repr__(self):
        return "%s/%s/%s" % (self.namespace, self.type, self.identity)

    def typename(self):
        return "%s/%s" % (self.namespace, self.type)


class AnyStateHandle(object):
    def __init__(self, any_bytes):
        self.any = None
        self.value_bytes = any_bytes
        self.modified = False
        self.deleted = False

    def bytes(self):
        if self.deleted:
            raise AssertionError("can not obtain the bytes of a delete handle")
        if self.modified:
            return self.value.SerializeToString()
        else:
            return self.value_bytes

    def unpack(self, into_class):
        if self.value:
            into_ref = into_class()
            self.value.Unpack(into_ref)
            return into_ref
        else:
            return None

    def pack(self, message):
        any = Any()
        any.Pack(message)
        self.value = any

    @property
    def value(self):
        """returns the current value of this state"""
        if self.deleted:
            return None
        if self.any:
            return self.any
        if not self.value_bytes:
            return None
        self.any = Any()
        self.any.ParseFromString(self.value_bytes)
        return self.any

    @value.setter
    def value(self, any):
        """updates this value to the supplied value, and also marks this state as modified"""
        self.any = any
        self.value_bytes = None
        self.modified = True
        self.deleted = False

    @value.deleter
    def value(self):
        """marks this state as deleted and also as modified"""
        self.any = None
        self.value_bytes = None
        self.deleted = True
        self.modified = True


class Expiration(object):
    class Mode(Enum):
        AFTER_INVOKE = 0
        AFTER_WRITE = 1

    def __init__(self, expire_after: timedelta, expire_mode: Mode=Mode.AFTER_INVOKE):
        self.expire_mode = expire_mode
        self.expire_after_millis = self.total_milliseconds(expire_after)
        if self.expire_after_millis <= 0:
            raise ValueError("expire_after_millis must be a positive number.")

    @staticmethod
    def total_milliseconds(expire_after: timedelta):
        return int(expire_after.total_seconds() * 1000.0)


class AfterInvoke(Expiration):
    def __init__(self, expire_after: timedelta):
        super().__init__(expire_after, expire_mode=Expiration.Mode.AFTER_INVOKE)


class AfterWrite(Expiration):
    def __init__(self, expire_after: timedelta):
        super().__init__(expire_after, expire_mode=Expiration.Mode.AFTER_WRITE)


class StateSpec(object):
    def __init__(self, name, expire_after: Expiration=None):
        self.name = name
        self.expiration = expire_after
        if not name:
            raise ValueError("state name must be provided")


class StateRegistrationError(Exception):
    pass


class StatefulFunction(object):
    def __init__(self, fun, state_specs: List[StateSpec], expected_messages=None):
        self.known_messages = expected_messages[:] if expected_messages else None
        self.func = fun
        if not fun:
            raise ValueError("function code is missing.")
        self.registered_state_specs = {}
        if state_specs:
            for state_spec in state_specs:
                if state_spec.name in self.registered_state_specs:
                    raise StateRegistrationError("duplicate registered state name: " + state_spec.name)
                self.registered_state_specs[state_spec.name] = state_spec

    def unpack_any(self, any: Any):
        if self.known_messages is None:
            return None
        for cls in self.known_messages:
            if any.Is(cls.DESCRIPTOR):
                instance = cls()
                any.Unpack(instance)
                return instance

        raise ValueError("Unknown message type " + any.type_url)


def parse_typename(typename):
    """parses a string of type namespace/type into a tuple of (namespace, type)"""
    if typename is None:
        raise ValueError("function type must be provided")
    idx = typename.rfind("/")
    if idx < 0:
        raise ValueError("function type must be of the from namespace/name")
    namespace = typename[:idx]
    if not namespace:
        raise ValueError("function type's namespace must not be empty")
    type = typename[idx + 1:]
    if not type:
        raise ValueError("function type's name must not be empty")
    return namespace, type


def deduce_protobuf_types(fn):
    """
    Try to extract the class names that are attached as the typing annotation.

    :param fn: the function with the annotated parameters.
    :return: a list of classes or None.
    """
    spec = inspect.getfullargspec(fn)
    if not spec:
        return None
    if len(spec.args) != 2:
        raise TypeError("A stateful function must have two arguments: a context and a message. but got ", spec.args)
    message_arg_name = spec.args[1]  # has to be the second element
    if message_arg_name not in spec.annotations:
        return None
    message_annotation = spec.annotations[message_arg_name]
    if inspect.isclass(message_annotation):
        return [message_annotation]
    try:
        # it is not a class, then it is only allowed to be
        # typing.SpecialForm('Union')
        return list(message_annotation.__args__)
    except Exception:
        return None


class StatefulFunctions:
    def __init__(self):
        self.functions = {}

    def register(self, typename: str, fun, state_specs: List[StateSpec]=None):
        """registers a StatefulFunction function instance, under the given namespace with the given function type. """
        if fun is None:
            raise ValueError("function instance must be provided")
        namespace, type = parse_typename(typename)
        expected_messages = deduce_protobuf_types(fun)
        self.functions[(namespace, type)] = StatefulFunction(fun, state_specs, expected_messages)

    def bind(self, typename, states: List[StateSpec]=None):
        """wraps a StatefulFunction instance with a given namespace and type.
           for example:
            s = StateFun()

            @s.define("com.foo.bar/greeter")
            def greeter(context, message):
                print("Hi there")

            This would add an invokable stateful function that can accept messages
            sent to "com.foo.bar/greeter".
         """

        def wrapper(function):
            self.register(typename, function, states)
            return function

        return wrapper

    def for_type(self, namespace, type):
        return self.functions[(namespace, type)]


def kafka_egress_record(topic: str, value, key: str = None):
    """
    Build a ProtobufMessage that can be emitted to a Kafka generic egress.

    :param topic: The Kafka destination topic for that record
    :param key: the utf8 encoded string key to produce (can be empty)
    :param value: the Protobuf value to produce
    :return: A Protobuf message representing the record to be produced via the Kafka generic egress.
    """
    if not topic:
        raise ValueError("A destination Kafka topic is missing")
    if not value:
        raise ValueError("Missing value")
    record = KafkaProducerRecord()
    record.topic = topic
    record.value_bytes = value.SerializeToString()
    if key is not None:
        record.key = key
    return record

def kinesis_egress_record(stream: str, value, partition_key: str, explicit_hash_key: str = None):
    """
    Build a ProtobufMessage that can be emitted to a Kinesis generic egress.

    :param stream: The AWS Kinesis destination stream for that record
    :param partition_key: the utf8 encoded string partition key to use
    :param value: the Protobuf value to produce
    :param explicit_hash_key: a utf8 encoded string explicit hash key to use (can be empty)
    :return: A Protobuf message representing the record to be produced to AWS Kinesis via the Kinesis generic egress.
    """
    if not stream:
        raise ValueError("Missing destination Kinesis stream")
    if not value:
        raise ValueError("Missing value")
    if not partition_key:
        raise ValueError("Missing partition key")
    record = KinesisEgressRecord()
    record.stream = stream
    record.value_bytes = value.SerializeToString()
    record.partition_key = partition_key
    if explicit_hash_key is not None:
        record.explicit_hash_key = explicit_hash_key
    return record
