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
import asyncio
import unittest
from datetime import timedelta

from google.protobuf.json_format import MessageToDict
from google.protobuf.any_pb2 import Any

from tests.examples_pb2 import LoginEvent, SeenCount
from statefun.request_reply_pb2 import ToFunction, FromFunction, TypedValue
from statefun import RequestReplyHandler, AsyncRequestReplyHandler
from statefun import StatefulFunctions, StateSpec, AfterWrite, StateRegistrationError
from statefun import kafka_egress_record, kinesis_egress_record


class InvocationBuilder(object):
    """builder for the ToFunction message"""

    def __init__(self):
        self.to_function = ToFunction()

    def with_target(self, ns, type, id):
        InvocationBuilder.set_address(ns, type, id, self.to_function.invocation.target)
        return self

    def with_state(self, name, value=None):
        state = self.to_function.invocation.state.add()
        state.state_name = name
        if value:
            state.state_value.CopyFrom(self.to_typed_value_any_state(value))
        return self

    def with_invocation(self, arg, caller=None):
        invocation = self.to_function.invocation.invocations.add()
        if caller:
            (ns, type, id) = caller
            InvocationBuilder.set_address(ns, type, id, invocation.caller)
        invocation.argument.CopyFrom(self.to_typed_value(arg))
        return self

    def SerializeToString(self):
        return self.to_function.SerializeToString()

    @staticmethod
    def to_typed_value(proto_msg):
        any = Any()
        any.Pack(proto_msg)
        typed_value = TypedValue()
        typed_value.typename = any.type_url
        typed_value.value = any.value
        return typed_value

    @staticmethod
    def to_typed_value_any_state(proto_msg):
        any = Any()
        any.Pack(proto_msg)
        typed_value = TypedValue()
        typed_value.typename = "type.googleapis.com/google.protobuf.Any"
        typed_value.value = any.SerializeToString()
        return typed_value

    @staticmethod
    def set_address(namespace, type, id, address):
        address.namespace = namespace
        address.type = type
        address.id = id


def round_trip(functions: StatefulFunctions, to: InvocationBuilder) -> dict:
    handler = RequestReplyHandler(functions)
    f = FromFunction()
    f.ParseFromString(handler(to.SerializeToString()))
    return MessageToDict(f, preserving_proto_field_name=True)


def async_round_trip(functions: StatefulFunctions, to: InvocationBuilder) -> dict:
    handler = AsyncRequestReplyHandler(functions)

    in_bytes = to.SerializeToString()
    future = handler(in_bytes)
    out_bytes = asyncio.get_event_loop().run_until_complete(future)

    f = FromFunction()
    f.ParseFromString(out_bytes)
    return MessageToDict(f, preserving_proto_field_name=True)


def json_at(nested_structure: dict, path):
    try:
        for next in path:
            nested_structure = next(nested_structure)
        return nested_structure
    except KeyError:
        return None


def key(s: str):
    return lambda dict: dict[s]


def nth(n):
    return lambda list: list[n]


NTH_OUTGOING_MESSAGE = lambda n: [key("invocation_result"), key("outgoing_messages"), nth(n)]
NTH_STATE_MUTATION = lambda n: [key("invocation_result"), key("state_mutations"), nth(n)]
NTH_DELAYED_MESSAGE = lambda n: [key("invocation_result"), key("delayed_invocations"), nth(n)]
NTH_EGRESS = lambda n: [key("invocation_result"), key("outgoing_egresses"), nth(n)]
NTH_MISSING_STATE_SPEC = lambda n: [key("incomplete_invocation_context"), key("missing_values"), nth(n)]


class RequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/greeter',
            states=[StateSpec('seen')])
        def fun(context, message):
            # state access
            seen = context.state('seen').unpack(SeenCount)
            seen.seen += 1
            context.state('seen').pack(seen)

            # regular state access
            seenAny = context['seen']
            seenAny.Unpack(seen)

            # sending and replying
            context.pack_and_reply(seen)

            any = Any()
            any.type_url = 'type.googleapis.com/k8s.demo.SeenCount'
            context.send("bar.baz/foo", "12345", any)

            # delayed messages
            context.send_after(timedelta(hours=1), "night/owl", "1", any)

            # egresses
            context.send_egress("foo.bar.baz/my-egress", any)
            context.pack_and_send_egress("foo.bar.baz/my-egress", seen)

            # kafka egress
            context.pack_and_send_egress("sdk/kafka",
                                         kafka_egress_record(topic="hello", key=u"hello world", value=seen))
            context.pack_and_send_egress("sdk/kafka",
                                         kafka_egress_record(topic="hello", value=seen))

            # AWS Kinesis generic egress
            context.pack_and_send_egress("sdk/kinesis",
                                         kinesis_egress_record(
                                             stream="hello",
                                             partition_key=u"hello world",
                                             value=seen,
                                             explicit_hash_key=u"1234"))
            context.pack_and_send_egress("sdk/kinesis",
                                         kinesis_egress_record(
                                             stream="hello",
                                             partition_key=u"hello world",
                                             value=seen))

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen", seen)

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = round_trip(functions, builder)

        # assert first outgoing message
        first_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(first_out_message['target']['namespace'], 'org.foo')
        self.assertEqual(first_out_message['target']['type'], 'greeter-java')
        self.assertEqual(first_out_message['target']['id'], '0')
        self.assertEqual(first_out_message['argument']['typename'], 'type.googleapis.com/k8s.demo.SeenCount')

        # assert second outgoing message
        second_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(1))
        self.assertEqual(second_out_message['target']['namespace'], 'bar.baz')
        self.assertEqual(second_out_message['target']['type'], 'foo')
        self.assertEqual(second_out_message['target']['id'], '12345')
        self.assertEqual(second_out_message['argument']['typename'], 'type.googleapis.com/k8s.demo.SeenCount')

        # assert state mutations
        first_mutation = json_at(result_json, NTH_STATE_MUTATION(0))
        self.assertEqual(first_mutation['mutation_type'], 'MODIFY')
        self.assertEqual(first_mutation['state_name'], 'seen')
        self.assertIsNotNone(first_mutation['state_value'])

        # assert delayed
        first_delayed = json_at(result_json, NTH_DELAYED_MESSAGE(0))
        self.assertEqual(int(first_delayed['delay_in_ms']), 1000 * 60 * 60)

        # assert egresses
        first_egress = json_at(result_json, NTH_EGRESS(0))
        self.assertEqual(first_egress['egress_namespace'], 'foo.bar.baz')
        self.assertEqual(first_egress['egress_type'], 'my-egress')
        self.assertEqual(first_egress['argument']['typename'], 'type.googleapis.com/k8s.demo.SeenCount')

    def test_integration_incomplete_context(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/bar',
            states=[
                StateSpec('seen'),
                StateSpec('missing_state_1'),
                StateSpec('missing_state_2', expire_after=AfterWrite(timedelta(milliseconds=2000)))
            ])
        def fun(context, message):
            pass

        #
        # build an invocation that provides only 'seen' state
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "bar", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen", seen)

        builder.with_invocation(Any(), None)

        #
        # invoke
        #
        result_json = round_trip(functions, builder)

        #
        # assert indicated missing states
        #
        missing_state_1_spec = json_at(result_json, NTH_MISSING_STATE_SPEC(0))
        self.assertEqual(missing_state_1_spec['state_name'], 'missing_state_1')

        missing_state_2_spec = json_at(result_json, NTH_MISSING_STATE_SPEC(1))
        self.assertEqual(missing_state_2_spec['state_name'], 'missing_state_2')
        self.assertEqual(missing_state_2_spec['expiration_spec']['mode'], 'AFTER_WRITE')
        self.assertEqual(missing_state_2_spec['expiration_spec']['expire_after_millis'], '2000')

    def test_integration_access_non_registered_state(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/bar',
            states=[StateSpec('seen')])
        def fun(context, message):
            ignored = context['non_registered_state']

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "bar", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen", seen)

        builder.with_invocation(Any(), None)

        #
        # assert error is raised on invoke
        #
        with self.assertRaises(StateRegistrationError):
            round_trip(functions, builder)


class AsyncRequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        functions = StatefulFunctions()

        @functions.bind('org.foo/greeter')
        async def fun(context, message):
            any = Any()
            any.type_url = 'type.googleapis.com/k8s.demo.SeenCount'
            context.send("bar.baz/foo", "12345", any)

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = async_round_trip(functions, builder)

        # assert outgoing message
        second_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(second_out_message['target']['namespace'], 'bar.baz')
        self.assertEqual(second_out_message['target']['type'], 'foo')
        self.assertEqual(second_out_message['target']['id'], '12345')
        self.assertEqual(second_out_message['argument']['typename'], 'type.googleapis.com/k8s.demo.SeenCount')

    def test_integration_incomplete_context(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/bar',
            states=[
                StateSpec('seen'),
                StateSpec('missing_state_1'),
                StateSpec('missing_state_2', expire_after=AfterWrite(timedelta(milliseconds=2000)))
            ])
        async def fun(context, message):
            pass

        #
        # build an invocation that provides only 'seen' state
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "bar", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen", seen)

        builder.with_invocation(Any(), None)

        #
        # invoke
        #
        result_json = async_round_trip(functions, builder)

        #
        # assert indicated missing states
        #
        missing_state_1_spec = json_at(result_json, NTH_MISSING_STATE_SPEC(0))
        self.assertEqual(missing_state_1_spec['state_name'], 'missing_state_1')

        missing_state_2_spec = json_at(result_json, NTH_MISSING_STATE_SPEC(1))
        self.assertEqual(missing_state_2_spec['state_name'], 'missing_state_2')
        self.assertEqual(missing_state_2_spec['expiration_spec']['mode'], 'AFTER_WRITE')
        self.assertEqual(missing_state_2_spec['expiration_spec']['expire_after_millis'], '2000')

    def test_integration_access_non_registered_state(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/bar',
            states=[StateSpec('seen')])
        async def fun(context, message):
            ignored = context['non_registered_state']

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "bar", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen", seen)

        builder.with_invocation(Any(), None)

        #
        # assert error is raised on invoke
        #
        with self.assertRaises(StateRegistrationError):
            async_round_trip(functions, builder)
