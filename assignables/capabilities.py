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


class SchemaCapability(assignable.Applicable):
    """ Capability used for Schemas """

    def apply(self, schema, access, transaction, data_by_principal: dict):
        return data_by_principal  # TODO: Check method in superclass


class Capabilities(assignable.Applicables):

    def select_for_apply(self, subclass: type[assignable.Assignable] | None, schema, access, transaction, data) -> list[assignable.Assignable]:
        """ select assignable for .apply()
            We select the required capabilities according to access.mode, according
            to the capabilities supplied by this schema. 
        """
        # join the capabilities from each mode:
        required_capabilities = SchemaCapability.capabilities_for_modes(access.modes)
        byname = {c for c, v in self._by_classname.items() if subclass is None or isinstance(
            v, subclass)}  # select name of those in given subclass
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        return [self._by_classname[c] for c in byname.intersection(required_capabilities)]


class Validate(SchemaCapability):
    supports_modes = frozenset()


NoCapabilities = Capabilities()
