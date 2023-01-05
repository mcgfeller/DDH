""" Executable Capabilities, especailly for Schemas """
from __future__ import annotations

import abc
import enum
import typing

import pydantic
from utils.pydantic_utils import DDHbaseModel

from . import (errors, versions, permissions)


class Capability(DDHbaseModel):
    ...


SchemaCapability = typing.ForwardRef('SchemaCapability')
Capabilities = typing.ForwardRef('Capabilities')


class SchemaCapability(Capability):
    """ Capability used for Schemas """
    Capabilities: typing.ClassVar[dict[str, SchemaCapability]] = {}
    supports_modes: typing.ClassVar[set[permissions.AccessMode]]  # supports_modes is a mandatory class variable
    by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    @classmethod
    def __init_subclass__(cls):
        """ register subclass as .Capabilities """
        instance = cls()
        cls.Capabilities[cls.__name__] = instance
        # register instance in a set of mode supporters:
        [cls.by_modes.setdefault(m, set()).add(cls.__name__) for m in cls.supports_modes]
        return

    @classmethod
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode]) -> set[Capabilities]:
        """ return the capabilities required for the access modes """
        caps = set.union(set(), *[c for m in modes if (c := cls.by_modes.get(m))])
        return {Capabilities(c) for c in caps}  # to Enum

    def apply(self, schema, access, transaction, data):
        return data


class Validate(SchemaCapability):
    supports_modes = set()


class Anonymize(SchemaCapability):
    supports_modes = {permissions.AccessMode.anonymous}


class Pseudonymize(Anonymize):
    supports_modes = {permissions.AccessMode.pseudonym}


# Enum with all available Capabilities:
Capabilities = enum.Enum('Capabilities', [(n, n) for n in SchemaCapability.Capabilities], type=str, module=__name__)
