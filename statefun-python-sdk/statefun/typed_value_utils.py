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

from statefun.core import AnyStateHandle
from statefun.request_reply_pb2 import TypedValue

#
# Utility methods to covert back and forth from Protobuf Any to our TypedValue.
# TODO this conversion needs to take place only because the Python SDK still works with Protobuf Any's
# TODO this would soon go away by letting the SDK work directly with TypedValues.
#

def to_proto_any(typed_value: TypedValue):
    proto_any = Any()
    proto_any.type_url = typed_value.typename
    proto_any.value = typed_value.value
    return proto_any

def from_proto_any(proto_any: Any):
    typed_value = TypedValue()
    typed_value.typename = proto_any.type_url
    typed_value.value = proto_any.value
    return typed_value

def from_proto_any_state(any_state_handle: AnyStateHandle):
    typed_value = TypedValue()
    typed_value.typename = any_state_handle.typename()
    typed_value.value = any_state_handle.bytes()
    return typed_value

def to_proto_any_state(typed_value: TypedValue) -> AnyStateHandle:
    return AnyStateHandle(typed_value.value)
