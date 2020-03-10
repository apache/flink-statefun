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
from statefun import StatefulFunctions

functions = StatefulFunctions()

@functions.bind("example/caller")
def caller_function(context, message):
    """A simple stateful function that sends a message to the user with id `user1`"""

    user = User()
    user.user_id = "user1"
    user.name = "Seth"

    envelope = Any()
    envelope.Pack(user)

    context.send("example/hello", user.user_id, envelope)
