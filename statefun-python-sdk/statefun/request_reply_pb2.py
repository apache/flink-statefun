# -*- coding: utf-8 -*-
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
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: request-reply.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='request-reply.proto',
  package='org.apache.flink.statefun.flink.core.polyglot',
  syntax='proto3',
  serialized_options=b'\n7org.apache.flink.statefun.flink.core.polyglot.generatedP\001',
  serialized_pb=b'\n\x13request-reply.proto\x12-org.apache.flink.statefun.flink.core.polyglot\x1a\x19google/protobuf/any.proto\"6\n\x07\x41\x64\x64ress\x12\x11\n\tnamespace\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t\x12\n\n\x02id\x18\x03 \x01(\t\"\xcf\x04\n\nToFunction\x12\x66\n\ninvocation\x18\x64 \x01(\x0b\x32P.org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequestH\x00\x1a\x39\n\x0ePersistedValue\x12\x12\n\nstate_name\x18\x01 \x01(\t\x12\x13\n\x0bstate_value\x18\x02 \x01(\x0c\x1a|\n\nInvocation\x12\x46\n\x06\x63\x61ller\x18\x01 \x01(\x0b\x32\x36.org.apache.flink.statefun.flink.core.polyglot.Address\x12&\n\x08\x61rgument\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\x1a\x94\x02\n\x16InvocationBatchRequest\x12\x46\n\x06target\x18\x01 \x01(\x0b\x32\x36.org.apache.flink.statefun.flink.core.polyglot.Address\x12W\n\x05state\x18\x02 \x03(\x0b\x32H.org.apache.flink.statefun.flink.core.polyglot.ToFunction.PersistedValue\x12Y\n\x0binvocations\x18\x03 \x03(\x0b\x32\x44.org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationB\t\n\x07request\"\x90\x0e\n\x0c\x46romFunction\x12k\n\x11invocation_result\x18\x64 \x01(\x0b\x32N.org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponseH\x00\x12\x80\x01\n\x1dincomplete_invocation_context\x18\x65 \x01(\x0b\x32W.org.apache.flink.statefun.flink.core.polyglot.FromFunction.IncompleteInvocationContextH\x00\x1a\xe1\x01\n\x16PersistedValueMutation\x12v\n\rmutation_type\x18\x01 \x01(\x0e\x32_.org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation.MutationType\x12\x12\n\nstate_name\x18\x02 \x01(\t\x12\x13\n\x0bstate_value\x18\x03 \x01(\x0c\"&\n\x0cMutationType\x12\n\n\x06\x44\x45LETE\x10\x00\x12\n\n\x06MODIFY\x10\x01\x1a|\n\nInvocation\x12\x46\n\x06target\x18\x01 \x01(\x0b\x32\x36.org.apache.flink.statefun.flink.core.polyglot.Address\x12&\n\x08\x61rgument\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\x1a\x98\x01\n\x11\x44\x65layedInvocation\x12\x13\n\x0b\x64\x65lay_in_ms\x18\x01 \x01(\x03\x12\x46\n\x06target\x18\x02 \x01(\x0b\x32\x36.org.apache.flink.statefun.flink.core.polyglot.Address\x12&\n\x08\x61rgument\x18\x03 \x01(\x0b\x32\x14.google.protobuf.Any\x1a\x66\n\rEgressMessage\x12\x18\n\x10\x65gress_namespace\x18\x01 \x01(\t\x12\x13\n\x0b\x65gress_type\x18\x02 \x01(\t\x12&\n\x08\x61rgument\x18\x03 \x01(\x0b\x32\x14.google.protobuf.Any\x1a\xb6\x03\n\x12InvocationResponse\x12k\n\x0fstate_mutations\x18\x01 \x03(\x0b\x32R.org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation\x12\x61\n\x11outgoing_messages\x18\x02 \x03(\x0b\x32\x46.org.apache.flink.statefun.flink.core.polyglot.FromFunction.Invocation\x12j\n\x13\x64\x65layed_invocations\x18\x03 \x03(\x0b\x32M.org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation\x12\x64\n\x11outgoing_egresses\x18\x04 \x03(\x0b\x32I.org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage\x1a\xcd\x01\n\x0e\x45xpirationSpec\x12\x63\n\x04mode\x18\x01 \x01(\x0e\x32U.org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec.ExpireMode\x12\x1b\n\x13\x65xpire_after_millis\x18\x02 \x01(\x03\"9\n\nExpireMode\x12\x08\n\x04NONE\x10\x00\x12\x0f\n\x0b\x41\x46TER_WRITE\x10\x01\x12\x10\n\x0c\x41\x46TER_INVOKE\x10\x02\x1a\x8d\x01\n\x12PersistedValueSpec\x12\x12\n\nstate_name\x18\x01 \x01(\t\x12\x63\n\x0f\x65xpiration_spec\x18\x02 \x01(\x0b\x32J.org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec\x1a\x85\x01\n\x1bIncompleteInvocationContext\x12\x66\n\x0emissing_values\x18\x01 \x03(\x0b\x32N.org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueSpecB\n\n\x08responseB;\n7org.apache.flink.statefun.flink.core.polyglot.generatedP\x01\x62\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_any__pb2.DESCRIPTOR,])



_FROMFUNCTION_PERSISTEDVALUEMUTATION_MUTATIONTYPE = _descriptor.EnumDescriptor(
  name='MutationType',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation.MutationType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='DELETE', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='MODIFY', index=1, number=1,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=1192,
  serialized_end=1230,
)
_sym_db.RegisterEnumDescriptor(_FROMFUNCTION_PERSISTEDVALUEMUTATION_MUTATIONTYPE)

_FROMFUNCTION_EXPIRATIONSPEC_EXPIREMODE = _descriptor.EnumDescriptor(
  name='ExpireMode',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec.ExpireMode',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='NONE', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='AFTER_WRITE', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='AFTER_INVOKE', index=2, number=2,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=2207,
  serialized_end=2264,
)
_sym_db.RegisterEnumDescriptor(_FROMFUNCTION_EXPIRATIONSPEC_EXPIREMODE)


_ADDRESS = _descriptor.Descriptor(
  name='Address',
  full_name='org.apache.flink.statefun.flink.core.polyglot.Address',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='namespace', full_name='org.apache.flink.statefun.flink.core.polyglot.Address.namespace', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='type', full_name='org.apache.flink.statefun.flink.core.polyglot.Address.type', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='id', full_name='org.apache.flink.statefun.flink.core.polyglot.Address.id', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=97,
  serialized_end=151,
)


_TOFUNCTION_PERSISTEDVALUE = _descriptor.Descriptor(
  name='PersistedValue',
  full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.PersistedValue',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='state_name', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.PersistedValue.state_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='state_value', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.PersistedValue.state_value', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=272,
  serialized_end=329,
)

_TOFUNCTION_INVOCATION = _descriptor.Descriptor(
  name='Invocation',
  full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.Invocation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='caller', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.Invocation.caller', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='argument', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.Invocation.argument', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=331,
  serialized_end=455,
)

_TOFUNCTION_INVOCATIONBATCHREQUEST = _descriptor.Descriptor(
  name='InvocationBatchRequest',
  full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='target', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequest.target', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='state', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequest.state', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='invocations', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequest.invocations', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=458,
  serialized_end=734,
)

_TOFUNCTION = _descriptor.Descriptor(
  name='ToFunction',
  full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='invocation', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.invocation', index=0,
      number=100, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_TOFUNCTION_PERSISTEDVALUE, _TOFUNCTION_INVOCATION, _TOFUNCTION_INVOCATIONBATCHREQUEST, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='request', full_name='org.apache.flink.statefun.flink.core.polyglot.ToFunction.request',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=154,
  serialized_end=745,
)


_FROMFUNCTION_PERSISTEDVALUEMUTATION = _descriptor.Descriptor(
  name='PersistedValueMutation',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='mutation_type', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation.mutation_type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='state_name', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation.state_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='state_value', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation.state_value', index=2,
      number=3, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _FROMFUNCTION_PERSISTEDVALUEMUTATION_MUTATIONTYPE,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1005,
  serialized_end=1230,
)

_FROMFUNCTION_INVOCATION = _descriptor.Descriptor(
  name='Invocation',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.Invocation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='target', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.Invocation.target', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='argument', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.Invocation.argument', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1232,
  serialized_end=1356,
)

_FROMFUNCTION_DELAYEDINVOCATION = _descriptor.Descriptor(
  name='DelayedInvocation',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='delay_in_ms', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation.delay_in_ms', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='target', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation.target', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='argument', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation.argument', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1359,
  serialized_end=1511,
)

_FROMFUNCTION_EGRESSMESSAGE = _descriptor.Descriptor(
  name='EgressMessage',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='egress_namespace', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage.egress_namespace', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='egress_type', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage.egress_type', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='argument', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage.argument', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1513,
  serialized_end=1615,
)

_FROMFUNCTION_INVOCATIONRESPONSE = _descriptor.Descriptor(
  name='InvocationResponse',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='state_mutations', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse.state_mutations', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='outgoing_messages', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse.outgoing_messages', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='delayed_invocations', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse.delayed_invocations', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='outgoing_egresses', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse.outgoing_egresses', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1618,
  serialized_end=2056,
)

_FROMFUNCTION_EXPIRATIONSPEC = _descriptor.Descriptor(
  name='ExpirationSpec',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='mode', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec.mode', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='expire_after_millis', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec.expire_after_millis', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _FROMFUNCTION_EXPIRATIONSPEC_EXPIREMODE,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=2059,
  serialized_end=2264,
)

_FROMFUNCTION_PERSISTEDVALUESPEC = _descriptor.Descriptor(
  name='PersistedValueSpec',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueSpec',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='state_name', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueSpec.state_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='expiration_spec', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueSpec.expiration_spec', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=2267,
  serialized_end=2408,
)

_FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT = _descriptor.Descriptor(
  name='IncompleteInvocationContext',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.IncompleteInvocationContext',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='missing_values', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.IncompleteInvocationContext.missing_values', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=2411,
  serialized_end=2544,
)

_FROMFUNCTION = _descriptor.Descriptor(
  name='FromFunction',
  full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='invocation_result', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.invocation_result', index=0,
      number=100, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='incomplete_invocation_context', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.incomplete_invocation_context', index=1,
      number=101, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_FROMFUNCTION_PERSISTEDVALUEMUTATION, _FROMFUNCTION_INVOCATION, _FROMFUNCTION_DELAYEDINVOCATION, _FROMFUNCTION_EGRESSMESSAGE, _FROMFUNCTION_INVOCATIONRESPONSE, _FROMFUNCTION_EXPIRATIONSPEC, _FROMFUNCTION_PERSISTEDVALUESPEC, _FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='response', full_name='org.apache.flink.statefun.flink.core.polyglot.FromFunction.response',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=748,
  serialized_end=2556,
)

_TOFUNCTION_PERSISTEDVALUE.containing_type = _TOFUNCTION
_TOFUNCTION_INVOCATION.fields_by_name['caller'].message_type = _ADDRESS
_TOFUNCTION_INVOCATION.fields_by_name['argument'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_TOFUNCTION_INVOCATION.containing_type = _TOFUNCTION
_TOFUNCTION_INVOCATIONBATCHREQUEST.fields_by_name['target'].message_type = _ADDRESS
_TOFUNCTION_INVOCATIONBATCHREQUEST.fields_by_name['state'].message_type = _TOFUNCTION_PERSISTEDVALUE
_TOFUNCTION_INVOCATIONBATCHREQUEST.fields_by_name['invocations'].message_type = _TOFUNCTION_INVOCATION
_TOFUNCTION_INVOCATIONBATCHREQUEST.containing_type = _TOFUNCTION
_TOFUNCTION.fields_by_name['invocation'].message_type = _TOFUNCTION_INVOCATIONBATCHREQUEST
_TOFUNCTION.oneofs_by_name['request'].fields.append(
  _TOFUNCTION.fields_by_name['invocation'])
_TOFUNCTION.fields_by_name['invocation'].containing_oneof = _TOFUNCTION.oneofs_by_name['request']
_FROMFUNCTION_PERSISTEDVALUEMUTATION.fields_by_name['mutation_type'].enum_type = _FROMFUNCTION_PERSISTEDVALUEMUTATION_MUTATIONTYPE
_FROMFUNCTION_PERSISTEDVALUEMUTATION.containing_type = _FROMFUNCTION
_FROMFUNCTION_PERSISTEDVALUEMUTATION_MUTATIONTYPE.containing_type = _FROMFUNCTION_PERSISTEDVALUEMUTATION
_FROMFUNCTION_INVOCATION.fields_by_name['target'].message_type = _ADDRESS
_FROMFUNCTION_INVOCATION.fields_by_name['argument'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_FROMFUNCTION_INVOCATION.containing_type = _FROMFUNCTION
_FROMFUNCTION_DELAYEDINVOCATION.fields_by_name['target'].message_type = _ADDRESS
_FROMFUNCTION_DELAYEDINVOCATION.fields_by_name['argument'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_FROMFUNCTION_DELAYEDINVOCATION.containing_type = _FROMFUNCTION
_FROMFUNCTION_EGRESSMESSAGE.fields_by_name['argument'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_FROMFUNCTION_EGRESSMESSAGE.containing_type = _FROMFUNCTION
_FROMFUNCTION_INVOCATIONRESPONSE.fields_by_name['state_mutations'].message_type = _FROMFUNCTION_PERSISTEDVALUEMUTATION
_FROMFUNCTION_INVOCATIONRESPONSE.fields_by_name['outgoing_messages'].message_type = _FROMFUNCTION_INVOCATION
_FROMFUNCTION_INVOCATIONRESPONSE.fields_by_name['delayed_invocations'].message_type = _FROMFUNCTION_DELAYEDINVOCATION
_FROMFUNCTION_INVOCATIONRESPONSE.fields_by_name['outgoing_egresses'].message_type = _FROMFUNCTION_EGRESSMESSAGE
_FROMFUNCTION_INVOCATIONRESPONSE.containing_type = _FROMFUNCTION
_FROMFUNCTION_EXPIRATIONSPEC.fields_by_name['mode'].enum_type = _FROMFUNCTION_EXPIRATIONSPEC_EXPIREMODE
_FROMFUNCTION_EXPIRATIONSPEC.containing_type = _FROMFUNCTION
_FROMFUNCTION_EXPIRATIONSPEC_EXPIREMODE.containing_type = _FROMFUNCTION_EXPIRATIONSPEC
_FROMFUNCTION_PERSISTEDVALUESPEC.fields_by_name['expiration_spec'].message_type = _FROMFUNCTION_EXPIRATIONSPEC
_FROMFUNCTION_PERSISTEDVALUESPEC.containing_type = _FROMFUNCTION
_FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT.fields_by_name['missing_values'].message_type = _FROMFUNCTION_PERSISTEDVALUESPEC
_FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT.containing_type = _FROMFUNCTION
_FROMFUNCTION.fields_by_name['invocation_result'].message_type = _FROMFUNCTION_INVOCATIONRESPONSE
_FROMFUNCTION.fields_by_name['incomplete_invocation_context'].message_type = _FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT
_FROMFUNCTION.oneofs_by_name['response'].fields.append(
  _FROMFUNCTION.fields_by_name['invocation_result'])
_FROMFUNCTION.fields_by_name['invocation_result'].containing_oneof = _FROMFUNCTION.oneofs_by_name['response']
_FROMFUNCTION.oneofs_by_name['response'].fields.append(
  _FROMFUNCTION.fields_by_name['incomplete_invocation_context'])
_FROMFUNCTION.fields_by_name['incomplete_invocation_context'].containing_oneof = _FROMFUNCTION.oneofs_by_name['response']
DESCRIPTOR.message_types_by_name['Address'] = _ADDRESS
DESCRIPTOR.message_types_by_name['ToFunction'] = _TOFUNCTION
DESCRIPTOR.message_types_by_name['FromFunction'] = _FROMFUNCTION
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Address = _reflection.GeneratedProtocolMessageType('Address', (_message.Message,), {
  'DESCRIPTOR' : _ADDRESS,
  '__module__' : 'request_reply_pb2'
  # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.Address)
  })
_sym_db.RegisterMessage(Address)

ToFunction = _reflection.GeneratedProtocolMessageType('ToFunction', (_message.Message,), {

  'PersistedValue' : _reflection.GeneratedProtocolMessageType('PersistedValue', (_message.Message,), {
    'DESCRIPTOR' : _TOFUNCTION_PERSISTEDVALUE,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.ToFunction.PersistedValue)
    })
  ,

  'Invocation' : _reflection.GeneratedProtocolMessageType('Invocation', (_message.Message,), {
    'DESCRIPTOR' : _TOFUNCTION_INVOCATION,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.ToFunction.Invocation)
    })
  ,

  'InvocationBatchRequest' : _reflection.GeneratedProtocolMessageType('InvocationBatchRequest', (_message.Message,), {
    'DESCRIPTOR' : _TOFUNCTION_INVOCATIONBATCHREQUEST,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.ToFunction.InvocationBatchRequest)
    })
  ,
  'DESCRIPTOR' : _TOFUNCTION,
  '__module__' : 'request_reply_pb2'
  # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.ToFunction)
  })
_sym_db.RegisterMessage(ToFunction)
_sym_db.RegisterMessage(ToFunction.PersistedValue)
_sym_db.RegisterMessage(ToFunction.Invocation)
_sym_db.RegisterMessage(ToFunction.InvocationBatchRequest)

FromFunction = _reflection.GeneratedProtocolMessageType('FromFunction', (_message.Message,), {

  'PersistedValueMutation' : _reflection.GeneratedProtocolMessageType('PersistedValueMutation', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_PERSISTEDVALUEMUTATION,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueMutation)
    })
  ,

  'Invocation' : _reflection.GeneratedProtocolMessageType('Invocation', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_INVOCATION,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.Invocation)
    })
  ,

  'DelayedInvocation' : _reflection.GeneratedProtocolMessageType('DelayedInvocation', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_DELAYEDINVOCATION,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.DelayedInvocation)
    })
  ,

  'EgressMessage' : _reflection.GeneratedProtocolMessageType('EgressMessage', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_EGRESSMESSAGE,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.EgressMessage)
    })
  ,

  'InvocationResponse' : _reflection.GeneratedProtocolMessageType('InvocationResponse', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_INVOCATIONRESPONSE,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.InvocationResponse)
    })
  ,

  'ExpirationSpec' : _reflection.GeneratedProtocolMessageType('ExpirationSpec', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_EXPIRATIONSPEC,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.ExpirationSpec)
    })
  ,

  'PersistedValueSpec' : _reflection.GeneratedProtocolMessageType('PersistedValueSpec', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_PERSISTEDVALUESPEC,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.PersistedValueSpec)
    })
  ,

  'IncompleteInvocationContext' : _reflection.GeneratedProtocolMessageType('IncompleteInvocationContext', (_message.Message,), {
    'DESCRIPTOR' : _FROMFUNCTION_INCOMPLETEINVOCATIONCONTEXT,
    '__module__' : 'request_reply_pb2'
    # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction.IncompleteInvocationContext)
    })
  ,
  'DESCRIPTOR' : _FROMFUNCTION,
  '__module__' : 'request_reply_pb2'
  # @@protoc_insertion_point(class_scope:org.apache.flink.statefun.flink.core.polyglot.FromFunction)
  })
_sym_db.RegisterMessage(FromFunction)
_sym_db.RegisterMessage(FromFunction.PersistedValueMutation)
_sym_db.RegisterMessage(FromFunction.Invocation)
_sym_db.RegisterMessage(FromFunction.DelayedInvocation)
_sym_db.RegisterMessage(FromFunction.EgressMessage)
_sym_db.RegisterMessage(FromFunction.InvocationResponse)
_sym_db.RegisterMessage(FromFunction.ExpirationSpec)
_sym_db.RegisterMessage(FromFunction.PersistedValueSpec)
_sym_db.RegisterMessage(FromFunction.IncompleteInvocationContext)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)