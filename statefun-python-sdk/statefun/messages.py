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

import typing

import statefun.wrapper_types as wrapper_types
from statefun.core import Type
from statefun.request_reply_pb2 import TypedValue
from statefun.utils import to_typed_value


class Message(object):
    __slots__ = ("target_typename", "target_id", "typed_value")

    def __init__(self, target_typename: str, target_id: str, typed_value: TypedValue):
        """
        A Stateful Functions Message.

        :param target_typename: The TypeName represented as a string of the form <namespace>/<name> of the
        target function.
        :param target_id: The id of the target function
        :param typed_value: The internal protobuf representation of the typed_value.
        """
        if not target_typename:
            raise ValueError("target_typename can not be missing")
        if not target_id:
            raise ValueError("target_id can not be missing")
        if not typed_value:
            raise ValueError("typed_value can not be missing")
        self.target_typename = target_typename
        self.target_id = target_id
        self.typed_value = typed_value

    def is_int(self):
        return self.is_type(wrapper_types.IntType)

    def as_int(self):
        return self.as_type(wrapper_types.IntType)

    def is_bool(self):
        return self.is_type(wrapper_types.BoolType)

    def as_bool(self) -> typing.Optional[bool]:
        return self.as_type(wrapper_types.BoolType)

    def is_long(self):
        return self.is_type(wrapper_types.LongType)

    def as_long(self) -> typing.Optional[int]:
        return self.as_type(wrapper_types.LongType)

    def is_string(self):
        return self.is_type(wrapper_types.StringType)

    def as_string(self) -> typing.Optional[str]:
        return self.as_type(wrapper_types.StringType)

    def is_float(self):
        return self.is_type(wrapper_types.FloatType)

    def as_float(self) -> typing.Optional[float]:
        return self.as_type(wrapper_types.FloatType)

    def is_double(self):
        return self.is_type(wrapper_types.DoubleType)

    def as_double(self) -> typing.Optional[float]:
        return self.as_type(wrapper_types.DoubleType)

    def is_type(self, tpe: Type) -> bool:
        return self.typed_value.typename == tpe.typename

    def value_typename(self) -> str:
        return self.typed_value.typename

    def raw_value(self) -> typing.Optional[bytes]:
        tv = self.typed_value
        return tv.value if tv.has_value else None

    def as_type(self, tpe: Type):
        tv = self.typed_value
        if tv.has_value:
            serializer = tpe.serializer()
            return serializer.deserialize(tv.value)
        else:
            return None


class EgressMessage(object):
    __slots__ = ("typename", "typed_value")

    def __init__(self, typename: str, typed_value: TypedValue):
        if not typename:
            raise ValueError("typename is missing")
        if not typed_value:
            raise ValueError("value is missing")
        self.typename = typename
        self.typed_value = typed_value

    def value_typename(self) -> str:
        return self.typed_value.typename

    def raw_value(self) -> typing.Optional[bytes]:
        tv = self.typed_value
        if tv.has_value:
            return tv.value
        else:
            return None


PRIMITIVE_GETTERS = {"int_value": wrapper_types.IntType,
                     "float_value": wrapper_types.FloatType,
                     "long_value": wrapper_types.LongType,
                     "str_value": wrapper_types.StringType,
                     "double_value": wrapper_types.DoubleType,
                     "bool_value": wrapper_types.BoolType}


def message_builder(target_typename: str, target_id: str, **kwargs) -> Message:
    """
    Build a Message that can be sent to any other function.
    :param target_typename: The TypeName represented as a string of the form <namespace>/<name> of the
           target function.
    :param target_id: The id of the target function
    :param kwargs: This specify the value type to attach to this message. The following arguments are supported:
                    int_value=<an int>,
                    float_value=<a float>
                    long_value=<a signed 64 bit integer>
                    str_value=<str>
                    double_value=<double>
                    bool_value=<bool>
                    ...
                    value=<arbitrary value>, value_type=<a StateFun Type for this value>
    :return: A Message object, that can be sent.
    """
    if len(kwargs) == 2:
        value, value_type = kwargs["value"], kwargs["value_type"]
    elif len(kwargs) == 1:
        # expecting: <type>_value : value
        # for example one of the following:
        #              int_value=1
        #              str_value="hello world"
        #              long_value= 5511
        type_keyword, value = next(iter(kwargs.items()))
        value_type = PRIMITIVE_GETTERS.get(type_keyword)
    else:
        raise TypeError(f"Wrong number of value keywords given: {kwargs}, there must be exactly one of:"
                        f"\nint_value=.."
                        f"\nfloat_value.."
                        f"\netc'"
                        f"\nor:"
                        f"\nvalue=.. ,value_type=.. ")
    if value is None:
        raise ValueError("value can not be missing")
    if not value_type:
        raise ValueError(
            "Could not deduce the value type, please specify the type explicitly. via passing: value=<the value>, "
            "value_type=<the type>")
    typed_value = to_typed_value(type=value_type, value=value)
    return Message(target_typename=target_typename, target_id=target_id, typed_value=typed_value)


def egress_message_builder(target_typename: str, value: typing.Any, value_type: Type):
    """
    Create a generic egress record.

    To use Kafka specific egress please use kafka_egress_message(), and for Kinesis please use
    kinesis_egress_message().
    """
    if not target_typename:
        raise ValueError("target typename is missing")
    if value is None:
        raise ValueError("value can not be missing")
    typed_value = to_typed_value(type=value_type, value=value)
    return EgressMessage(typename=target_typename, typed_value=typed_value)
