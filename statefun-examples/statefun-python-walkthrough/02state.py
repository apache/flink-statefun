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
from walkthrough_pb2 import Counter

functions = StatefulFunctions()


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
        pb_any.Unpack(counter)
        counter.value = 1
        pb_any.Pack(counter)
        context['counter'] = pb_any


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


if __name__ == "__main__":
    from example_utils import flask_server
    flask_server("/statefun", functions)
