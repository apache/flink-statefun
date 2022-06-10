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
from statefun import StatefulFunctions, ValueSpec, IntType, StringType


# noinspection PyUnusedLocal
class StatefulFunctionsTestCase(unittest.TestCase):

    def test_example(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename="org.foo/greeter",
            specs=[ValueSpec(name='seen_count', type=IntType)])
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertFalse(fun.is_async)
        self.assertIsNotNone(fun.storage_spec)

    def test_async(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename="org.foo/greeter",
            specs=[ValueSpec(name='seen_count', type=IntType)])
        async def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertTrue(fun.is_async)
        self.assertIsNotNone(fun.storage_spec)

    def test_state_spec(self):
        functions = StatefulFunctions()

        foo = ValueSpec(name='foo', type=IntType)
        bar = ValueSpec(name='bar', type=StringType)

        @functions.bind(typename="org.foo/greeter", specs=[foo, bar])
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertListEqual(fun.storage_spec.specs, [foo, bar])

    def test_stateless(self):
        functions = StatefulFunctions()

        @functions.bind(typename="org.foo/greeter")
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertListEqual(fun.storage_spec.specs, [])

    def test_duplicate_state(self):
        functions = StatefulFunctions()

        with self.assertRaises(ValueError):
            @functions.bind(
                typename="org.foo/greeter",
                specs=[ValueSpec(name="bar", type=IntType), ValueSpec(name="bar", type=IntType)])
            def foo(context, message):
                pass

    def test_wrong_signature(self):
        functions = StatefulFunctions()

        with self.assertRaises(ValueError):
            @functions.bind(
                typename="org.foo/greeter",
                specs=[ValueSpec(name="bar", type=IntType)])
            def foo(message):  # missing context
                pass
