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
from datetime import timedelta
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("flink/delayed")
def delayed(context, message):
    """A function that sends itself a message after a delay """

    if message.Is(Message.DESCRIPTOR):
        print("Hello!")

        envelope = Any()
        envelope.Pack(DelayedMessage())

        context.send_after(
            context.self.typename(),
            context.self.identity,
            timedelta(minutes=1),
            envelope)
        return

    if message.Is(DelayedMessage.DESCRIPTOR):
        print("Hello from the future!")
        return