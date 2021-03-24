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

import typing
from dataclasses import dataclass

from statefun.core import ValueSpec, Type
from statefun.request_reply_pb2 import TypedValue, ToFunction

from statefun.utils import to_typed_value, from_typed_value


@dataclass(frozen=True, repr=True, eq=True, order=False)
class StorageSpec:
    make_instance: typing.Any
    names: typing.FrozenSet[str]
    specs: typing.List[ValueSpec]


@dataclass(frozen=True, eq=False, repr=False, order=False)
class Resolution:
    missing_specs: typing.Union[None, typing.List[ValueSpec]]
    storage: typing.Union[None, typing.Any]  # this holds the storage instance.


class Cell(object):
    __slots__ = ("tpe", "typed_value", "dirty")

    def __init__(self, tpe: Type, typed_value: typing.Optional[TypedValue]):
        # read only
        self.tpe = tpe
        # mutable
        self.typed_value = typed_value
        self.dirty = False

    def get(self):
        typed_value = self.typed_value
        return from_typed_value(self.tpe, typed_value)

    def set(self, val):
        if val is None:
            raise ValueError('provided value must not be None. To delete a value, please use del.')
        tpe = self.tpe
        typed_value = to_typed_value(tpe, val)
        self.typed_value = typed_value
        self.dirty = True

    def delete(self):
        self.typed_value = None
        self.dirty = True


# self.cells: typing.Dict[str, Cell] = {name: Cell(name, tpe, vals[name]) for name, tpe in types.items()}


def storage_constructor(self, cells: typing.Dict[str, Cell]):
    self._cells = cells


def property_named(name):
    """
    Creates a property, that delegates all the operations to an instance
    of a HiddenClass, that is expected to be member of target class (the class that
    this property will be part of).

    :param name: the name of this field
    :return: a property as described above.
    """

    def fget(self):
        cell: Cell = self._cells.get(name)
        if not cell:
            raise AttributeError(name)
        return cell.get()

    def fset(self, val):
        cell: Cell = self._cells.get(name)
        if not cell:
            raise AttributeError(name)
        cell.set(val)

    def fdel(self):
        cell: Cell = self._cells.get(name)
        if not cell:
            raise AttributeError(name)
        cell.delete()

    return property(fget, fset, fdel)


def make_address_storage_spec(specs: typing.List[ValueSpec]) -> StorageSpec:
    """
    Creates an StorageSpec from user supplied value specs.
    :param specs: a list of specs as supplied by the user.
    :return: a StorageSpec.
    """
    props = {"__init__": storage_constructor, "__slots__": ["_cells"]}
    for spec in specs:
        if spec.name in props:
            raise ValueError("duplicate registered value name: " + spec.name)
        props[spec.name] = property_named(spec.name)
    cls = type("GeneratedAddressedScopedStorage", (object,), props)
    return StorageSpec(make_instance=cls,
                       names=frozenset(spec.name for spec in specs),
                       specs=specs)


def resolve(storage: StorageSpec,
            values: typing.List[ToFunction.PersistedValue]) -> Resolution:
    """
    Resolve the registered specs and the actually received values.

    :param storage: a storage factory
    :param values: the actually received values
    :return: a Resolution result, that might have either a list of missing specs
    (specs that were defined by the user but didn't arrived from StateFun) or a
     successful resolution, with an instance of an addressed scoped storage.
    """
    #
    # index the actually received values (TypedValue) by name.
    #
    received: typing.Dict[str, TypedValue] = {state.state_name: state.state_value for state in values}
    #
    # see if any of the specs are missing.
    #
    missing_keys = storage.names - received.keys()
    if missing_keys:
        # keep the missing specs in exactly the same order as they were originally defined.
        # This is not strictly required from the protocol point of view, but it makes it a bit easier
        # to troubleshoot.
        missing = [spec for spec in storage.specs if spec.name in missing_keys]
        return Resolution(missing_specs=missing, storage=None)
    else:
        cells: typing.Dict[str, Cell] = {spec.name: Cell(tpe=spec.type, typed_value=received[spec.name]) for spec in
                                         storage.specs}
        s = storage.make_instance(cells)
        return Resolution(missing_specs=None, storage=s)
