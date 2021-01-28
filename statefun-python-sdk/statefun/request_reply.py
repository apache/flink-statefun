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
from datetime import timedelta

from google.protobuf.any_pb2 import Any

from statefun.core import SdkAddress
from statefun.core import Expiration
from statefun.core import parse_typename
from statefun.core import StateRegistrationError

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction
from statefun.typed_value_utils import to_proto_any, from_proto_any, to_proto_any_state, from_proto_any_state

class InvocationContext:
    def __init__(self, functions):
        self.functions = functions
        self.missing_state_specs = None
        self.batch = None
        self.context = None
        self.target_function = None

    def setup(self, request_bytes):
        to_function = ToFunction()
        to_function.ParseFromString(request_bytes)
        #
        # setup
        #
        target_address = to_function.invocation.target
        target_function = self.functions.for_type(target_address.namespace, target_address.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_function)

        # for each state spec defined in target function
        #    if state name is in request -> add to Batch Context
        #    if state name is not in request -> add to missing_state_specs
        provided_state_values = self.provided_state_values(to_function)
        missing_state_specs = []
        resolved_state_values = {}
        for state_name, state_spec in target_function.registered_state_specs.items():
            if state_name in provided_state_values:
                resolved_state_values[state_name] = provided_state_values[state_name]
            else:
                missing_state_specs.append(state_spec)

        self.batch = to_function.invocation.invocations
        self.context = BatchContext(target_address, resolved_state_values)
        self.target_function = target_function
        if missing_state_specs:
            self.missing_state_specs = missing_state_specs

    def complete(self):
        from_function = FromFunction()
        if not self.missing_state_specs:
            invocation_result = from_function.invocation_result
            context = self.context
            self.add_mutations(context, invocation_result)
            self.add_outgoing_messages(context, invocation_result)
            self.add_delayed_messages(context, invocation_result)
            self.add_egress(context, invocation_result)
            # reset the state for the next invocation
            self.batch = None
            self.context = None
            self.target_function = None
            # return the result
        else:
            incomplete_context = from_function.incomplete_invocation_context
            self.add_missing_state_specs(self.missing_state_specs, incomplete_context)
        return from_function.SerializeToString()

    @staticmethod
    def provided_state_values(to_function):
        return {s.state_name: to_proto_any_state(s.state_value) for s in to_function.invocation.state}

    @staticmethod
    def add_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages
        for typename, id, message in context.messages:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.argument.CopyFrom(from_proto_any(message))

    @staticmethod
    def add_mutations(context, invocation_result):
        for name, handle in context.states.items():
            if not handle.modified:
                continue
            mutation = invocation_result.state_mutations.add()

            mutation.state_name = name
            if handle.deleted:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('DELETE')
            else:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('MODIFY')
                mutation.state_value.CopyFrom(from_proto_any_state(handle))

    @staticmethod
    def add_delayed_messages(context, invocation_result):
        delayed_invocations = invocation_result.delayed_invocations
        for delay, typename, id, message in context.delayed_messages:
            outgoing = delayed_invocations.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.delay_in_ms = delay
            outgoing.argument.CopyFrom(from_proto_any(message))

    @staticmethod
    def add_egress(context, invocation_result):
        outgoing_egresses = invocation_result.outgoing_egresses
        for typename, message in context.egresses:
            outgoing = outgoing_egresses.add()

            namespace, type = parse_typename(typename)
            outgoing.egress_namespace = namespace
            outgoing.egress_type = type
            outgoing.argument.CopyFrom(from_proto_any(message))

    @staticmethod
    def add_missing_state_specs(missing_state_specs, incomplete_context_response):
        missing_values = incomplete_context_response.missing_values
        for state_spec in missing_state_specs:
            missing_value = missing_values.add()
            missing_value.state_name = state_spec.name

            # TODO see the comment in typed_value_utils.from_proto_any_state on
            # TODO the reason to use this specific typename
            missing_value.type_typename = "type.googleapis.com/google.protobuf.Any"

            protocol_expiration_spec = FromFunction.ExpirationSpec()
            sdk_expiration_spec = state_spec.expiration
            if not sdk_expiration_spec:
                protocol_expiration_spec.mode = FromFunction.ExpirationSpec.ExpireMode.NONE
            else:
                protocol_expiration_spec.expire_after_millis = sdk_expiration_spec.expire_after_millis
                if sdk_expiration_spec.expire_mode is Expiration.Mode.AFTER_INVOKE:
                    protocol_expiration_spec.mode = FromFunction.ExpirationSpec.ExpireMode.AFTER_INVOKE
                elif sdk_expiration_spec.expire_mode is Expiration.Mode.AFTER_WRITE:
                    protocol_expiration_spec.mode = FromFunction.ExpirationSpec.ExpireMode.AFTER_WRITE
                else:
                    raise ValueError("Unexpected state expiration mode.")
            missing_value.expiration_spec.CopyFrom(protocol_expiration_spec)


class RequestReplyHandler:
    def __init__(self, functions):
        self.functions = functions

    def __call__(self, request_bytes):
        ic = InvocationContext(self.functions)
        ic.setup(request_bytes)
        if not ic.missing_state_specs:
            self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            any_arg = to_proto_any(invocation.argument)
            unpacked = target_function.unpack_any(any_arg)
            if not unpacked:
                fun(context, any_arg)
            else:
                fun(context, unpacked)


class AsyncRequestReplyHandler:
    def __init__(self, functions):
        self.functions = functions

    async def __call__(self, request_bytes):
        ic = InvocationContext(self.functions)
        ic.setup(request_bytes)
        if not ic.missing_state_specs:
            await self.handle_invocation(ic)
        return ic.complete()

    @staticmethod
    async def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            any_arg = to_proto_any(invocation.argument)
            unpacked = target_function.unpack_any(any_arg)
            if not unpacked:
                await fun(context, any_arg)
            else:
                await fun(context, unpacked)


class BatchContext(object):
    def __init__(self, target, states):
        self.states = states
        # remember own address
        self.address = SdkAddress(target.namespace, target.type, target.id)
        # the caller address would be set for each individual invocation in the batch
        self.caller = None
        # outgoing messages
        self.messages = []
        self.delayed_messages = []
        self.egresses = []

    def prepare(self, invocation):
        """setup per invocation """
        if invocation.caller:
            caller = invocation.caller
            self.caller = SdkAddress(caller.namespace, caller.type, caller.id)
        else:
            self.caller = None

    # --------------------------------------------------------------------------------------
    # state access
    # --------------------------------------------------------------------------------------

    def state(self, name):
        if name not in self.states:
            raise StateRegistrationError(
                'unknown state name ' + name + '; states need to be explicitly registered when binding functions.')
        return self.states[name]

    def __getitem__(self, name):
        return self.state(name).value

    def __delitem__(self, name):
        state = self.state(name)
        del state.value

    def __setitem__(self, name, value):
        state = self.state(name)
        state.value = value

    # --------------------------------------------------------------------------------------
    # messages
    # --------------------------------------------------------------------------------------

    def send(self, typename: str, id: str, message: Any):
        """
        Send a message to a function of type and id.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        out = (typename, id, message)
        self.messages.append(out)

    def pack_and_send(self, typename: str, id: str, message):
        """
        Send a Protobuf message to a function.

        This variant of send, would first pack this message
        into a google.protobuf.Any and then send it.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to pack into an Any and the send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send(typename, id, any)

    def reply(self, message: Any):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        caller = self.caller
        if not caller:
            raise AssertionError(
                "Unable to reply without a caller. Was this message was sent directly from an ingress?")
        self.send(caller.typename(), caller.identity, message)

    def pack_and_reply(self, message):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        any = Any()
        any.Pack(message)
        self.reply(any)

    def send_after(self, delay: timedelta, typename: str, id: str, message: Any):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not delay:
            raise ValueError("missing delay")
        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        duration_ms = int(delay.total_seconds() * 1000.0)
        out = (duration_ms, typename, id, message)
        self.delayed_messages.append(out)

    def pack_and_send_after(self, delay: timedelta, typename: str, id: str, message):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_after(delay, typename, id, any)

    def send_egress(self, typename, message: Any):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not typename:
            raise ValueError("missing type name")
        if not message:
            raise ValueError("missing message")
        self.egresses.append((typename, message))

    def pack_and_send_egress(self, typename, message):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_egress(typename, any)
