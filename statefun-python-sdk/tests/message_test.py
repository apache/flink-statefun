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

from statefun import message_builder


class MessageTestCase(unittest.TestCase):

    def test_example(self):
        m = message_builder(target_typename="foo/bar", target_id="a", int_value=1)

        self.assertTrue(m.is_int())
        self.assertEqual(m.as_int(), 1)

    def test_with_type(self):
        m = message_builder(target_typename="foo/bar", target_id="a", value=5.0, value_type=statefun.FloatType)
        self.assertTrue(m.is_float())
        self.assertEqual(m.as_float(), 5.0)

    def test_kafka_egress(self):
        record = statefun.kafka_egress_message(typename="foo/bar", topic="topic", value=1337420)

        self.assertEqual(record.typed_value.typename, "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord")
        self.assertTrue(record.typed_value.has_value)
