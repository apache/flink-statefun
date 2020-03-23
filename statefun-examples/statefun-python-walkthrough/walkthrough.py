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

from statefun import StatefulFunctions, kafka_egress_record
from google.protobuf.any_pb2 import Any

#
# @functions is the entry point, that allows us to register
# stateful functions identified via a namespace and a name pair
# of the form "<namespace>/<name>".
#
from walkthrough_pb2 import HelloReply, Hello, Counter, AnotherHello, Event

functions = StatefulFunctions()


#
# The following statement binds the Python function instance hello to a namespaced name
# "walkthrough/hello". This is also known as a function type, in stateful functions terms.
# i.e. the function type of hello is FunctionType(namespace="walkthrough", type="hello")
# messages that would be address to this function type, would be dispatched to this function instance.
#
@functions.bind("walkthrough/hello")
def hello(context, message):
    print(message)


# -----------------------------------------------------------------------------------------------------------------
# Message Types
# -----------------------------------------------------------------------------------------------------------------


@functions.bind("walkthrough/any")
def any_example(context, any_message):
    # messages sent to a Python function are always packed into a google.protobuf.Any
    # (https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/Any.html)
    # Therefore the first thing we need to do is to unpack it.
    if not any_message.Is(Hello.DESCRIPTOR):
        raise TypeError('Unexpected message type')

    hello = Hello()
    any_message.Unpack(hello)
    print(hello)


@functions.bind("walkthrough/type-hint")
def typehint(context, message: Hello):
    # Although messages that are  sent to a Python function are always packed into a google.protobuf.Any
    # StateFun can deduce type hints and can unpack the message for you automatically.
    print(message.world)


@functions.bind("walkthrough/union-type-hint")
def union_type_hint(context, message: typing.Union[Hello, AnotherHello]):
    # StateFun can deduce type hints and can unpack the message for you automatically, even
    # when you are expecting more than one message type.
    print(message)  # <-- would be either an instance of Hello or an instance of AnotherHello


@functions.bind("walkthrough/state_access")
def state1(context, message):
    # state can be accessed directly by getting the state name (as registered in a module.yaml). remember that the
    # state has to be a valid Protocol Buffers message, and has to be packed into a google.protobuf.Any
    pb_any = context['counter']
    if pb_any:
        # state was previously stored for this address
        counter = Counter()
        pb_any.Unpack(counter)
        counter.value += 1
        pb_any.Pack(counter)
        context['counter'] = pb_any
    else:
        # state was not stored for this address
        counter = Counter()
        counter.value = 1
        pb_any = Any()
        pb_any.Pack(counter)
        context['counter'] = pb_any


# -----------------------------------------------------------------------------------------------------------------
# State management
# -----------------------------------------------------------------------------------------------------------------

@functions.bind("walkthrough/state_access_unpack")
def state2(context, message):
    # statefun can help you to unpack/pack the values directly, removing some of the boilerplate
    # associated with google.protobuf.Any.
    counter = context.state('counter').unpack(Counter)
    if counter:
        counter.value += 1
    else:
        counter = Counter()
        counter.value = 1
    context.state('counter').pack(counter)


@functions.bind("walkthrough/state_access_del")
def state3(context, message):
    # state can be deleted easily by using the del keyword.
    del context['counter']


# -----------------------------------------------------------------------------------------------------------------
# Sending Messages
# -----------------------------------------------------------------------------------------------------------------

@functions.bind("walkthrough/send")
def send(context, message):
    # context allows you to send messages to other functions, as long as you
    # know their address. An address is composed of a function type and an id.
    any = Any()
    any.Pack(Hello())
    context.send("walkthrough/reply", "some-id", any)  # see reply() below.

    # you can also use the convenience alternative, that would pack the argument to a google.protobuf.Any
    context.pack_and_send("walkthrough/reply", "some-id", Hello())


@functions.bind("walkthrough/reply")
def reply(context, message):
    # directly reply to the sender!
    reply = HelloReply()
    reply.message = "This is a reply!"
    context.pack_and_reply(reply)


@functions.bind("walkthrough/egress")
def egress(context, message):
    # send a message to an external system via an egress. Egresses needs to be defined in a module.yaml
    # and can be referenced by type.
    # The following two lines prepare a message to send to the pre-built Kafka egress.
    key = context.address.identity  # use the identity part of our own address as the target Kafka key.
    record = kafka_egress_record(topic="events", key=key, value=Event())
    context.pack_and_send_egress("walkthrough/events-egress", record)


if __name__ == "__main__":
    from example_utils import flask_server

    flask_server("/statefun", functions)
