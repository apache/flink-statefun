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

from google.protobuf.any_pb2 import Any

from statefun.core import StatefulFunctions, StatefulFunction
from tests.examples_pb2 import LoginEvent

class StatefulFunctionsTestCase(unittest.TestCase):

    def test_example(self):
        functions = StatefulFunctions()

        @functions.bind("org.foo/greeter")
        def greeter(context, message):
            pass

        @functions.bind("org.foo/echo")
        def echo(context, message):
            pass

        self.assertIn(("org.foo", "greeter"), functions.functions)
        self.assertIn(("org.foo", "echo"), functions.functions)

    def test_type_deduction(self):
        functions = StatefulFunctions()

        @functions.bind("org.foo/greeter")
        def greeter(context, message: int):
            pass

        x: StatefulFunction = functions.functions[("org.foo", "greeter")]
        self.assertEqual(x.known_messages, [int])

    def test_unpacking(self):
        functions = StatefulFunctions()

        @functions.bind("org.foo/greeter")
        def greeter(context, message: LoginEvent):
            pass

        greeter_fn = functions.functions[("org.foo", "greeter")]

        # pack the function argument as an Any
        argument = LoginEvent()
        any_argument = Any()
        any_argument.Pack(argument)

        # unpack Any automatically
        unpacked_argument = greeter_fn.unpack_any(any_argument)

        self.assertEqual(argument, unpacked_argument)
