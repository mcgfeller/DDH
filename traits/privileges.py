""" Parametrized Privileges, e.g., for DApps """


import enum
import typing
import abc

import pydantic

from core import errors, trait

_Privilege = typing.ForwardRef('_Privilege')  # type: ignore
_DAppPrivilege = typing.ForwardRef('_DAppPrivilege')  # type: ignore


class _Privilege(trait.Trait):
    ...


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
    urls: frozenset[pydantic.AnyUrl] = frozenset()

    def merge(self, other: _Ports) -> typing.Self | None:
        """ return the stronger of self and other traits, creating a new combined 
            Traits.
        """
        r = super().merge(other)
        if r is None:
            return r
        else:
            if r == other:
                return r
            else:
                return self.__class__(urls=r.urls | other.urls)


class IncomingURL(_Ports):
    """ may be invoked from the listed urls """
    ...


class OutgoingURL(_Ports):
    """ may invoke listed urls """
    ...


class SensitiveDataRead(_DAppPrivilege):
    """ may read data that is designated sensitive in Schema """
    ...


class DAppPrivileges(trait.Traits):
    pass


NoPrivileges = DAppPrivileges()
