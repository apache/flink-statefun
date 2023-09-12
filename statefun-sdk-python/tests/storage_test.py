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

from statefun import IntType, StringType
from statefun.storage import *
from statefun.utils import to_typed_value


class PbPersistedValueLike:

    def __init__(self, name, val, tpe):
        self.state_name = name
        self.state_value = to_typed_value(tpe, val)


class StorageTestCase(unittest.TestCase):

    def test_example(self):
        specs = [ValueSpec(name="a", type=IntType), ValueSpec(name="b", type=StringType)]
        storage_spec = make_address_storage_spec(specs)

        # simulate values received from the StateFun cluster
        values = [PbPersistedValueLike("a", 1, IntType), PbPersistedValueLike("b", "hello", StringType)]

        # resolve spec and values
        resolution = resolve(storage_spec, "example/func", values)
        store = resolution.storage

        self.assertEqual(store.a, 1)
        self.assertEqual(store.b, "hello")

    def test_failed_resolution(self):
        specs = [ValueSpec(name="a", type=IntType), ValueSpec(name="b", type=StringType)]
        storage_spec = make_address_storage_spec(specs)

        # simulate values received from the StateFun cluster
        values = []

        # resolve spec and values
        resolution = resolve(storage_spec, "example/func", values)
        self.assertListEqual(resolution.missing_specs, specs)

    def test_partial_failed_resolution(self):
        specs = [ValueSpec(name="a", type=IntType), ValueSpec(name="b", type=StringType)]
        storage_spec = make_address_storage_spec(specs)

        # simulate values received from the StateFun cluster
        values = [PbPersistedValueLike("a", 1, IntType)]

        # resolve spec and values
        resolution = resolve(storage_spec, "example/func", values)
        self.assertListEqual(resolution.missing_specs, specs[1:])

    def test_ignore_unknown(self):
        specs = [ValueSpec(name="a", type=IntType)]
        storage_spec = make_address_storage_spec(specs)

        # simulate values received from the StateFun cluster
        values = [PbPersistedValueLike("a", 1, IntType), PbPersistedValueLike("b", "hello", StringType)]

        # resolve spec and values
        resolution = resolve(storage_spec, "example/func", values)
        store = resolution.storage

        self.assertEqual(store.a, 1)
        with self.assertRaises(AttributeError):
            print(store.b)

    def test_attribute_manipulation(self):
        store = store_from(ValueSpec("a", IntType),
                           PbPersistedValueLike("a", 1, IntType))

        store.a += 1
        self.assertEqual(store.a, 2)

        store.a = 1337
        self.assertEqual(store.a, 1337)

        del store.a
        self.assertIsNone(store.a)

        store.a = 0
        self.assertEqual(store.a, 0)

    def test_no_modifications(self):
        store = store_from(ValueSpec("a", IntType),
                           PbPersistedValueLike("a", 1, IntType))

        # noinspection PyUnusedLocal
        unused_a = store.a

        cell = store._cells["a"]
        self.assertFalse(cell.dirty)

    def test_modification(self):
        store = store_from(ValueSpec("a", IntType),
                           PbPersistedValueLike("a", 1, IntType))

        cell = store._cells["a"]

        store.a = 23249425
        self.assertTrue(cell.dirty)
        self.assertTrue(cell.typed_value.has_value)

        modified_a = IntType.serializer().deserialize(cell.typed_value.value)
        self.assertEqual(modified_a, 23249425)

    def test_deletion(self):
        store = store_from(ValueSpec("a", IntType),
                           PbPersistedValueLike("a", 1, IntType))

        cell = store._cells["a"]

        del store.a
        self.assertTrue(cell.dirty)
        self.assertIsNone(cell.typed_value)

    def test_missing_val(self):
        store = store_from(ValueSpec("a", IntType), PbPersistedValueLike("a", None, IntType))
        self.assertIsNone(store.a)

        store.a = 121314

        cell = store._cells["a"]
        self.assertTrue(cell.dirty)
        self.assertIsNotNone(cell.typed_value)
        self.assertTrue(cell.typed_value.has_value)

    def test_stateless(self):
        store = store_from()
        self.assertTrue(len(store._cells) == 0)

    def test_reference_equals(self):
        store = store_from(ValueSpec("a", StringType), PbPersistedValueLike("a", "123", StringType))

        a = store.a
        b = store.a

        self.assertEqual(id(a), id(b))

    def test_reset(self):
        store = store_from(ValueSpec("a", StringType), PbPersistedValueLike("a", "123", StringType))
        
        a = store.a
        cell = store._cells["a"]

        # reset is required when store.a is accessed in fnA, perhaps modified but not committed (i.e. store.a = a)
        # and we want fnB in the same batch to have the correct original value of a
        cell.reset()

        b = store.a
        self.assertNotEqual(id(a), id(b))

def store_from(*args):
    """test helper that creates an already resolved store from specs and pb values."""
    specs = []
    vals = []
    for arg in args:
        if isinstance(arg, ValueSpec):
            specs.append(arg)
        else:
            vals.append(arg)
    storage_spec = make_address_storage_spec(specs)
    resolution = resolve(storage_spec, "example/func", vals)
    return resolution.storage
