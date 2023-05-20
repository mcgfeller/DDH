""" Executable Restrictions, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key

from . import (errors, versions, permissions, schemas, transactions)


class Restriction(DDHbaseModel):
    def merge(self, other: Restriction) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        return self


SchemaRestriction = typing.ForwardRef('SchemaRestriction')
Restrictions = typing.ForwardRef('Restrictions')


class Restrictions(DDHbaseModel):
    """ A collection of Restriction """
    restrictions: list[Restriction] = []
    _by_name: dict[str, Restriction] = {}

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._by_name = {r.__class__.__name__: r for r in self.restrictions}

    def merge(self, other: Restrictions) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        if self._by_name == other._by_name:
            return self
        else:
            s = set(self._by_name)
            common = [self._by_name[common].merge(other._by_name[common]) for common in s.intersection(other._by_name)]
            s1 = [self._by_name[n] for n in s.difference(other._by_name)]
            s2 = [other._by_name[n] for n in set(other._by_name).difference(s)]
            r = self.__class__(restrictions=common+s1+s2)
            return r


class SchemaRestriction(Restriction):
    """ Restriction used for Schemas """

    may_overwrite: bool = pydantic.Field(
        default=False, description="restriction may be overwritten explicitly in lower schema")

    def merge(self, other: SchemaRestriction) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        if not other.may_overwrite:
            return other
        else:  # all other case are equal
            return self

    def apply(self, schema: schemas.AbstractSchema, access, transaction, data):
        return data


class MustReview(SchemaRestriction):
    by_roles: set[str] = set()

    def merge(self, other: MustReview) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        may_overwrite = self.may_overwrite and other.may_overwrite  # stronger
        if self.by_roles == other.by_roles and self.may_overwrite == may_overwrite:
            return self
        elif not self.by_roles and other.may_overwrite == may_overwrite:
            return other
        elif not other.by_roles and self.may_overwrite == may_overwrite:
            return self
        else:  # combine the roles
            return self.__class__(may_overwrite=may_overwrite, by_roles=self.by_roles | other.by_roles)


class MustHaveSensitivites(SchemaRestriction):
    """ This schema must have sensitivity annotations """
    ...


DefaultRestrictions = Restrictions()
HighPrivacyRestrictions = Restrictions(restrictions=[MustReview(), MustHaveSensitivites()])
HighestPrivacyRestrictions = Restrictions(restrictions=[MustReview(by_roles={'senior'}), MustHaveSensitivites()])
