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

@functions.bind("flink/count")
def count_greeter(context, message):
    """Function that greets a user based on
    the number of times it has been called"""
    user = User()
    message.Unpack(user)


    state = context["count"]
    if state is None:
        state = Any()
        state.Pack(Count(1))
        output = generate_message(1, user)
    else:
        counter = Count()
        state.Unpack(counter)
        counter.value += 1
        output = generate_message(counter.value, user)
        state.Pack(counter)

    context["count"] = state
    print(output)

def generate_message(count, user):
    if count == 1:
        return "Hello " + user.name
    elif count == 2:
        return "Hello again!"
    elif count == 3:
        return "Third time's the charm"
    else:
        return "Hello for the " + count + "th time"