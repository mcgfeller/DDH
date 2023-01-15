""" Parametrized Privileges, e.g., for DApps """
from __future__ import annotations

import enum
import typing
import abc

import pydantic
from utils.pydantic_utils import DDHbaseModel

from . import errors

_Privilege = typing.ForwardRef('_Privilege')  # type: ignore
_DAppPrivilege = typing.ForwardRef('_DAppPrivilege')  # type: ignore


class _Privilege(DDHbaseModel, typing.Hashable):
    class Config:
        frozen = True  # privileges are not mutable, and we need a hash function to build  a set


class _DAppPrivilege(_Privilege):
    """ Privileges a DApp may enjoy 
        - Whom to grant requested privileges? Review workflow?
    """
    Privileges: typing.ClassVar[dict[str, _Privilege]] = {}

    @classmethod
    def __init_subclass__(cls):
        """ register subclass as .Privileges """
        instance = cls()
        cls.Privileges[cls.__name__] = instance
        return


class System(_DAppPrivilege):
    """ System DApp, aka root -- Perhaps not, only specific services
    """
    ...


class _Ports(_DAppPrivilege):
    urls: tuple[pydantic.AnyUrl] = []


class IncomingURL(_Ports):
    """ may be invoked from the listed urls """
    ...


class OutgoingURL(_Ports):
    """ may invoke listed urls """
    ...


class SensitiveDataRead(_DAppPrivilege):
    """ may read data that is designated sensitive in Schema """
    ...


# Enum with all available Privileges:
DAppPrivileges = enum.Enum('DAppPrivileges', [(n, n) for n in _DAppPrivilege.Privileges], type=str, module=__name__)
