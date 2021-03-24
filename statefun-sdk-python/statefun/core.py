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
import abc
from dataclasses import dataclass
from datetime import timedelta
from keyword import iskeyword, kwlist


@dataclass(repr=True, eq=True, order=False, frozen=True)
class SdkAddress:
    namespace: str
    name: str
    id: str
    typename: str


class TypeSerializer(metaclass=abc.ABCMeta):
    __slots__ = ()

    """
    A base class for a TypeSerializer. A TypeSerializer is responsible of serialising and deserializing a specific
    value type.
    """

    @abc.abstractmethod
    def serialize(self, value) -> bytes:
        """
        Serialize the given value to bytes.

        :param value: the value to serialize.
        :return: a byte representation of the given value.
        """
        pass

    @abc.abstractmethod
    def deserialize(self, byte: bytes):
        """
        Deserialize a value from the given byte representation.

        :param byte: the bytes that represent the value to deseralize.
        :return: the deserialized value.
        """
        pass


class Type(metaclass=abc.ABCMeta):
    __slots__ = ("typename",)

    """
    A base representation of a Stateful Function value type.
    """

    def __init__(self, typename: str):
        """
        :param typename: a TypeName represented as a string of the form <namespace>/<name> of this type.
        """
        if not typename:
            raise ValueError("typename can not be missing")
        self.typename = typename

    @abc.abstractmethod
    def serializer(self) -> TypeSerializer:
        """
        :return: a serializer for this type.
        """
        pass


class ValueSpec(object):
    __slots__ = ("name", "type", "duration", "after_call", "after_write")

    def __init__(self,
                 name: str,
                 type: Type,
                 expire_after_call: timedelta = None,
                 expire_after_write: timedelta = None):
        """
        ValueSpec a specification of a persisted value.

        :param name: a user defined state name, used to represent this value. A name must be lower case, alphanumeric string
        without spaces, that starts with either a letter or an underscore (_)
        :param type: the value's Type. (see
        statefun.Type) :param expire_after_call: expire (remove) this value if a call to this function hasn't been
        made for the given duration.
        :param expire_after_write: expire this value if it wasn't written to for the given duration.
        """
        if not name:
            raise ValueError("name can not be missing.")
        if not name.isidentifier():
            raise ValueError(
                f"invalid name {name}. A spec name can only contains alphanumeric letters (a-z) and (0-9), "
                f"or underscores ( "
                f"_). A valid identifier cannot start with a number, or contain any spaces.")
        if iskeyword(name):
            forbidden = '\n'.join(kwlist)
            raise ValueError(
                f"invalid spec name {name} (Python SDK specifically). since {name} will result as an attribute on "
                f"context.store.\n"
                f"The following names are forbidden:\n {forbidden}")
        if not name.islower():
            raise ValueError(f"Only lower case names are allowed, {name} is given.")
        self.name = name
        if not type:
            raise ValueError("type can not be missing.")
        if not isinstance(type, Type):
            raise TypeError("type is not a StateFun type.")
        self.type = type
        if expire_after_call and expire_after_write:
            # both can not be set.
            raise ValueError("Either expire_after_call or expire_after_write can be set, but not both.")
        if expire_after_call:
            self.duration = int(expire_after_call.total_seconds() * 1000.0)
            self.after_call = True
            self.after_write = False
        elif expire_after_write:
            self.duration = int(expire_after_write.total_seconds() * 1000.0)
            self.after_call = False
            self.after_write = True
        else:
            self.duration = 0
            self.after_call = False
            self.after_write = False


def parse_typename(typename):
    """
    Parse a TypeName string into a namespace, type pair.
    :param typename: a string of the form <namespace>/<type>
    :return: a tuple of a namespace type.
    """
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


def simple_type(typename=None, serialize_fn=None, deserialize_fn=None) -> Type:
    """
    Create a user defined Type, simply by providing two functions. One for serializing a value to bytes, and one for
    deserializing the value from bytes.
    For example:

    import json
    tpe = simple_type("org.foo.bar/User", serialize_fn=json.dumps, deserialize_fn=json.loads)

    this defines a StateFun type (Type) of the typename org.foo.bar/User and it is a JSON object.

    :param typename: this type's TypeName.
    :param serialize_fn: a function that is able to serialize values of this type.
    :param deserialize_fn: a function that is able to deserialize bytes into values of this type.
    :return: a Type definition.
    """
    return SimpleType(typename, serialize_fn, deserialize_fn)


# ----------------------------------------------------------------------------------------------------------
# Internal
# ----------------------------------------------------------------------------------------------------------


class SimpleType(Type):
    __slots__ = ("typename", "_ser")

    def __init__(self, typename, serialize_fn, deserialize_fn):
        super().__init__(typename)
        if not serialize_fn:
            raise ValueError("serialize_fn is missing")
        if not deserialize_fn:
            raise ValueError("deserialize_fn is missing")
        self._ser = UserTypeSerializer(serialize_fn, deserialize_fn)

    def serializer(self) -> TypeSerializer:
        return self._ser


class UserTypeSerializer(TypeSerializer):
    __slots__ = ("serialize_fn", "deserialize_fn")

    def __init__(self, serialize_fn, deserialize_fn):
        self.serialize_fn = serialize_fn
        self.deserialize_fn = deserialize_fn

    def serialize(self, value):
        return self.serialize_fn(value)

    def deserialize(self, byte_string):
        return self.deserialize_fn(byte_string)
