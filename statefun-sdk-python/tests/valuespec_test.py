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

from statefun import IntType
from statefun.storage import *
from datetime import timedelta


class ValueSpecTestCase(unittest.TestCase):

    def test_example(self):
        a = ValueSpec(name="a", type=IntType)
        self.assertEqual(a.name, "a")
        self.assertEqual(a.type, IntType)
        self.assertFalse(a.after_write)
        self.assertFalse(a.after_call)

    def test_expire_after_access(self):
        a = ValueSpec(name="a", type=IntType, expire_after_call=timedelta(seconds=1))
        self.assertTrue(a.after_call)
        self.assertEqual(a.duration, 1000)

        self.assertFalse(a.after_write)

    def test_expire_after_write(self):
        a = ValueSpec(name="a", type=IntType, expire_after_write=timedelta(seconds=1))
        self.assertTrue(a.after_write)
        self.assertEqual(a.duration, 1000)

        self.assertFalse(a.after_call)

    def test_illegal_name(self):
        with self.assertRaises(ValueError):
            ValueSpec(name="-a", type=IntType, expire_after_call=timedelta(1))
        with self.assertRaises(ValueError):
            ValueSpec(name="def", type=IntType, expire_after_call=timedelta(1))
