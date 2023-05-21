""" Executable Restrictions, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key
from utils import utils

from . import (errors, versions, permissions, schemas, transactions)

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Restriction(DDHbaseModel):

    may_overwrite: bool = pydantic.Field(
        default=False, description="restriction may be overwritten explicitly in lower schema")

    def merge(self, other: Restriction) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        return self

    def apply(self, subject: Tsubject, restrictions: Restrictions, *a, **kw) -> Tsubject:
        return subject


SchemaRestriction = typing.ForwardRef('SchemaRestriction')
Restrictions = typing.ForwardRef('Restrictions')


class Restrictions(DDHbaseModel):
    """ A collection of Restriction """
    restrictions: list[Restriction] = []
    _by_name: dict[str, Restriction] = {}

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._by_name = {r.__class__.__name__: r for r in self.restrictions}

    def __contains__(self, restriction: type[Restriction]) -> bool:
        """ returns whether restriction class is in self """
        return restriction.__name__ in self._by_name

    def merge(self, other: Restrictions) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            Restrictions.
        """
        if self._by_name == other._by_name:
            return self
        else:  # merge those in common, then add those only in each set:
            s1 = set(self._by_name)
            s2 = set(other._by_name)
            common = [self._by_name[common].merge(other._by_name[common]) for common in s1 & s2]
            r1 = [r for n in s1 - s2 if not (r := self._by_name[n]).may_overwrite]  # only in self
            r2 = [other._by_name[n] for n in s2 - s1]  # only in other
            r = self.__class__(restrictions=common+r1+r2)
            return r

    def __add__(self, restriction: Restriction | list[Restriction]) -> typing.Self:
        """ add restriction by merging """
        return self.merge(self.__class__(restrictions=utils.ensure_tuple(restriction)))

    def apply(self, restriction_class: type[Restriction], subject: Tsubject, *a, **kw) -> Tsubject:
        """ apply restrictions in turn """
        for restriction in self.restrictions:
            if isinstance(restriction, restriction_class):
                subject = restriction.apply(subject, self, *a, **kw)
        return subject


class SchemaRestriction(Restriction):
    """ Restriction used for Schemas """

    def merge(self, other: SchemaRestriction) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        if not other.may_overwrite:
            return other
        else:  # all other case are equal
            return self

    def apply(self, schema: schemas.AbstractSchema, restrictions: Restrictions, access, transaction, ) -> schemas.AbstractSchema:
        return schema


class DataRestriction(SchemaRestriction):
    """ Restrictions on data for a schema """

    def apply(self, data: Tsubject, restrictions: Restrictions, schema: schemas.AbstractSchema, access, transaction) -> Tsubject:
        return data


class MustReview(SchemaRestriction):
    by_roles: set[str] = set()

    def merge(self, other: MustReview) -> typing.Self:
        """ return the stronger between self and other restrictions, creating a new combined 
            restriction. Any role is stronger than when no roles are specified. 
        """
        may_overwrite = self.may_overwrite and other.may_overwrite  # stronger
        if self.by_roles == other.by_roles and self.may_overwrite == may_overwrite:
            return self  # roles and may_overwrite match
        elif not self.by_roles and other.may_overwrite == may_overwrite:
            return other  # self has no roles, other may and wins:
        elif not other.by_roles and self.may_overwrite == may_overwrite:
            return self  # other has no roles, self may and wins:
        else:  # combine the roles - catches all possiblities, but makes new object
            return self.__class__(may_overwrite=may_overwrite, by_roles=self.by_roles | other.by_roles)


class MustHaveSensitivites(SchemaRestriction):
    """ This schema must have sensitivity annotations """
    ...


class MustValidate(DataRestriction):
    """ Data must be validated """

    def apply(self, schema: schemas.AbstractSchema, access, transaction, restrictions: Restrictions, data):
        no_extra = NoExtraElements in restrictions
        return data


class NoExtraElements(DataRestriction):
    """ Schema validation will reject extra elements not specified in the schema;
        marker applied by MustValidate
    """
    ...


DefaultRestrictions = Restrictions(restrictions=[MustValidate(), NoExtraElements()])
RootRestrictions = Restrictions(restrictions=[MustValidate(may_overwrite=True), NoExtraElements(may_overwrite=True)])
HighPrivacyRestrictions = DefaultRestrictions+[MustHaveSensitivites(), MustReview(),]
# Ensure we have a senior reviewer:
HighestPrivacyRestrictions = HighPrivacyRestrictions+MustReview(by_roles={'senior'})
