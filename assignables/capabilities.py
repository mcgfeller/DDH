""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key

from core import (errors, versions, permissions, schemas, transactions, assignable)
from backend import persistable

SchemaCapability = typing.ForwardRef('SchemaCapability')


class SchemaCapability(assignable.Assignable):
    """ Capability used for Schemas """
    supports_modes: typing.ClassVar[frozenset[permissions.AccessMode]]  # supports_modes is a mandatory class variable
    all_by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    @classmethod
    def __init_subclass__(cls):
        """ register all Capabilities by Mode """
        super().__init_subclass__()
        [cls.all_by_modes.setdefault(m, set()).add(cls.__name__) for m in cls.supports_modes]
        return

    @classmethod
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode]) -> set[str]:
        """ return the capabilities required for the access modes """
        caps = set.union(set(), *[c for m in modes if (c := cls.all_by_modes.get(m))])
        return caps

    def apply(self, schema, access, transaction, data_by_principal: dict):
        return data_by_principal  # TODO: Check method in superclass


class Capabilities(assignable.Assignables):

    def select_for_apply(self, subclass: type[assignable.Assignable] | None, schema, access, transaction, data) -> list[assignable.Assignable]:
        """ select assignable for .apply()
            Basisc selection is on subclass membership (if supplied), but may be refined.
        """
        # join the capabilities from each mode:
        required_capabilities = SchemaCapability.capabilities_for_modes(access.modes)
        byname = {c for c, v in self._by_classname.items() if isinstance(
            v, subclass)}  # select name of those in given subclass
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        return [self._by_classname[c] for c in byname.intersection(required_capabilities)]


class Validate(SchemaCapability):
    supports_modes = frozenset()


NoCapabilities = Capabilities()
