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
import unittest
from datetime import timedelta

from google.protobuf.json_format import MessageToDict

from statefun import *
from statefun.request_reply_pb2 import ToFunction, FromFunction
from statefun.utils import to_typed_value


class InvocationBuilder(object):
    """builder for the ToFunction message"""

    def __init__(self):
        self.to_function = ToFunction()

    def with_target(self, ns, type, id):
        InvocationBuilder.set_address(ns, type, id, self.to_function.invocation.target)
        return self

    def with_state(self, name, value=None, type=None):
        state = self.to_function.invocation.state.add()
        state.state_name = name
        if value is not None:
            state.state_value.CopyFrom(to_typed_value(type, value))
        return self

    def with_invocation(self, arg, tpe, caller=None):
        invocation = self.to_function.invocation.invocations.add()
        if caller:
            (ns, type, id) = caller
            InvocationBuilder.set_address(ns, type, id, invocation.caller)
        invocation.argument.CopyFrom(to_typed_value(tpe, arg))
        return self

    def SerializeToString(self):
        return self.to_function.SerializeToString()

    @staticmethod
    def set_address(namespace, type, id, address):
        address.namespace = namespace
        address.type = type
        address.id = id


def round_trip(functions: StatefulFunctions, to: InvocationBuilder) -> dict:
    handler = RequestReplyHandler(functions)

    in_bytes = to.SerializeToString()
    out_bytes = handler.handle_sync(in_bytes)

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
NTH_CANCELLATION_MESSAGE = lambda n: [key("invocation_result"), key("outgoing_delay_cancellations"), nth(n)]
NTH_EGRESS = lambda n: [key("invocation_result"), key("outgoing_egresses"), nth(n)]
NTH_MISSING_STATE_SPEC = lambda n: [key("incomplete_invocation_context"), key("missing_values"), nth(n)]


class RequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/greeter',
            specs=[ValueSpec(name='seen', type=IntType)])
        def fun(context: Context, message: Message):
            # messaging
            if message.is_string():
                unused = message.as_string()
                pass
                # print(f"A string message {message.as_string()}")

            # state access
            seen = context.storage.seen
            context.storage.seen += 1

            # sending
            context.send(message_builder(target_typename="org.foo/greeter-java",
                                         target_id="0",
                                         int_value=seen))
            # delayed messages
            context.send_after(timedelta(hours=1),
                               message_builder(target_typename="night/owl",
                                               target_id="1",
                                               str_value="hoo hoo"))

            # delayed with cancellation
            context.send_after(timedelta(hours=1),
                               message_builder(target_typename="night/owl",
                                               target_id="1",
                                               str_value="hoo hoo"),
                               cancellation_token="token-1234")

            context.cancel_delayed_message("token-1234")

            # kafka egresses
            context.send_egress(
                kafka_egress_message(typename="e/kafka",
                                     topic="out",
                                     key="abc",
                                     value=1337420))
            # kinesis egress
            context.send_egress(kinesis_egress_message(typename="e/kinesis",
                                                       stream="out",
                                                       partition_key="abc",
                                                       value="hello there"))

        #
        # build the invocation
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")
        builder.with_state("seen", 1, IntType)
        builder.with_invocation("Hello", StringType, ("org.foo", "greeter-java", "0"))

        #
        # invoke
        #
        result_json = round_trip(functions, builder)

        # assert first outgoing message
        first_out_message = json_at(result_json, NTH_OUTGOING_MESSAGE(0))
        self.assertEqual(first_out_message['target']['namespace'], 'org.foo')
        self.assertEqual(first_out_message['target']['type'], 'greeter-java')
        self.assertEqual(first_out_message['target']['id'], '0')
        self.assertEqual(first_out_message['argument']['typename'], 'io.statefun.types/int')

        # assert state mutations
        first_mutation = json_at(result_json, NTH_STATE_MUTATION(0))
        self.assertEqual(first_mutation['mutation_type'], 'MODIFY')
        self.assertEqual(first_mutation['state_name'], 'seen')
        self.assertIsNotNone(first_mutation['state_value'])

        # assert delayed
        first_delayed = json_at(result_json, NTH_DELAYED_MESSAGE(0))
        self.assertEqual(int(first_delayed['delay_in_ms']), 1000 * 60 * 60)

        # assert delayed with token
        second_delayed = json_at(result_json, NTH_DELAYED_MESSAGE(1))
        self.assertEqual(second_delayed['cancellation_token'], "token-1234")

        # assert cancellation
        first_cancellation = json_at(result_json, NTH_DELAYED_MESSAGE(2))
        self.assertTrue(first_cancellation['is_cancellation_request'])
        self.assertEqual(first_cancellation['cancellation_token'], "token-1234")

        # assert egresses
        first_egress = json_at(result_json, NTH_EGRESS(0))
        self.assertEqual(first_egress['egress_namespace'], 'e')
        self.assertEqual(first_egress['egress_type'], 'kafka')
        self.assertEqual(first_egress['argument']['typename'],
                         'type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord')

    def test_integration_incomplete_context(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename='org.foo/bar',
            specs=[
                ValueSpec(name='seen', type=IntType),
                ValueSpec('missing_state_1', type=StringType),
                ValueSpec('missing_state_2', type=FloatType, expire_after_write=timedelta(milliseconds=2000))
            ])
        def fun(context, message):
            pass

        #
        # build an invocation that provides only 'seen' state
        #
        builder = InvocationBuilder()
        builder.with_target("org.foo", "bar", "0")

        builder.with_state("seen")
        builder.with_invocation(arg=1, tpe=IntType)

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
