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

import statefun
from statefun.utils import to_typed_value


class TypeNameTestCase(unittest.TestCase):

    def assertRoundTrip(self, tpe, value):
        serializer = tpe.serializer()
        out = serializer.serialize(value)
        got = serializer.deserialize(out)
        self.assertEqual(got, value)

    def test_built_ins(self):
        self.assertRoundTrip(statefun.BoolType, True)
        self.assertRoundTrip(statefun.IntType, 0)
        self.assertRoundTrip(statefun.FloatType, float(0.5))
        self.assertRoundTrip(statefun.DoubleType, 1e-20)
        self.assertRoundTrip(statefun.LongType, 1 << 45)
        self.assertRoundTrip(statefun.StringType, "hello world")

    def test_json_type(self):
        import json
        tpe = statefun.simple_type(typename="org.foo.bar/UserJson",
                                   serialize_fn=json.dumps,
                                   deserialize_fn=json.loads)

        self.assertRoundTrip(tpe, {"name": "bob", "last": "mop"})

    def test_message(self):
        typed_value = to_typed_value(statefun.StringType, "hello world")
        msg = statefun.Message(target_typename="foo/bar", target_id="1", typed_value=typed_value)

        self.assertTrue(msg.is_string())
        self.assertEqual(msg.as_string(), "hello world")
