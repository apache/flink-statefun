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

from statefun import StatefulFunctions
from walkthrough_pb2 import Hello, AnotherHello

functions = StatefulFunctions()


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


if __name__ == "__main__":
    from example_utils import flask_server
    flask_server("/statefun", functions)
