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
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode]) -> list[SchemaCapability]:
        """ return the capabilities required for the access modes """
        return []


class Validate(SchemaCapability):
    supports_modes = set()


class Anonymize(SchemaCapability):
    supports_modes = {permissions.AccessMode.anonymous}


class Pseudonymize(Anonymize):
    supports_modes = {permissions.AccessMode.pseudonym}
