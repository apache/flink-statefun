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

from statefun import StatefulFunctions
from statefun import kafka_egress_record
from walkthrough_pb2 import HelloReply, Hello, Event
from google.protobuf.any_pb2 import Any

functions = StatefulFunctions()


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
