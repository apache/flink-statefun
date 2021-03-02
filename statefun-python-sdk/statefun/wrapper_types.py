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

from statefun.core import Type, TypeSerializer
from statefun.types_pb2 import *


class ProtobufWrappingTypeSerializer(TypeSerializer):
    __slots__ = ("wrapper",)

    def __init__(self, wrapper):
        self.wrapper = wrapper

    def serialize(self, value):
        instance = self.wrapper()
        instance.value = value
        return instance.SerializeToString()

    def deserialize(self, string):
        instance = self.wrapper()
        instance.ParseFromString(string)
        return instance.value


class ProtobufWrappingType(Type):
    __slots__ = ("wrapper",)

    def __init__(self, typename, wrapper_message_type):
        super().__init__(typename)
        self.wrapper = wrapper_message_type

    def serializer(self) -> TypeSerializer:
        return ProtobufWrappingTypeSerializer(self.wrapper)


BoolType = ProtobufWrappingType("io.statefun.types/bool", BooleanWrapper)
IntType = ProtobufWrappingType("io.statefun.types/int", IntWrapper)
FloatType = ProtobufWrappingType("io.statefun.types/float", FloatWrapper)
LongType = ProtobufWrappingType("io.statefun.types/long", LongWrapper)
DoubleType = ProtobufWrappingType("io.statefun.types/double", DoubleWrapper)
StringType = ProtobufWrappingType("io.statefun.types/string", StringWrapper)

PY_TYPE_TO_WRAPPER_TYPE = {
    int: IntType,
    bool: BoolType,
    float: FloatType,
    str: StringType
}

WRAPPER_TYPE_TO_PY_TYPE = {
    IntType: int,
    BoolType: bool,
    FloatType: float,
    StringType: str
}
