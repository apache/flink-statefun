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

from statefun.core import AnyStateHandle


def typed_any(type_url):
    any = Any()
    if type_url:
        any.type_url = type_url
    return any


def typed_any_bytes(type_url=None):
    return typed_any(type_url).SerializeToString()


class AnyHandleTestCase(unittest.TestCase):

    def test_example(self):
        handle = AnyStateHandle(typed_any_bytes("com/hello"))

        self.assertEqual(handle.value.type_url, "com/hello")

    def test_delete_handle(self):
        handle = AnyStateHandle(typed_any_bytes("com/hello"))

        del handle.value

        self.assertTrue(handle.value is None)

    def test_modify_handle(self):
        handle = AnyStateHandle(typed_any_bytes("com/hello"))

        handle.value = typed_any("com/world")

        self.assertEqual(handle.value.type_url, "com/world")

    def test_un_modified_bytes(self):
        handle = AnyStateHandle(typed_any_bytes("com/hello"))
        handle = AnyStateHandle(handle.bytes())

        self.assertEqual(handle.value.type_url, "com/hello")

    def test_modified_bytes(self):
        handle = AnyStateHandle(typed_any_bytes("com/hello"))

        handle.value = typed_any("com/world")
        handle = AnyStateHandle(handle.bytes())

        self.assertEqual(handle.value.type_url, "com/world")

    def test_missing_value(self):
        handle = AnyStateHandle(None)
        self.assertTrue(True)
