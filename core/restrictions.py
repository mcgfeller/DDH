""" Executable Restrictions, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime
import copy

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key
from utils import utils

from . import (errors, versions, permissions, schemas, transactions)

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Restriction(DDHbaseModel):

    may_overwrite: bool = pydantic.Field(
        default=False, description="restriction may be overwritten explicitly in lower schema")
    cancel: bool = pydantic.Field(
        default=False, description="cancels this restriction in merge; set using ~restriction")

    def __invert__(self) -> typing.Self:
        """ invert the cancel flag """
        r = copy.copy(self)
        r.cancel = not self.cancel
        return r

    def merge(self, other: Restriction) -> typing.Self | None:
        """ return the stronger of self and other restrictions if self.may_overwrite,
            or None if self.may_overwrite and cancels. 
        """
        if self.may_overwrite:
            if other.cancel:
                return None
            else:
                return other
        else:  # all other case are equal
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
            # merge, or cancel:
            common = [r for common in s1 & s2 if (r := self._by_name[common].merge(other._by_name[common])) is not None]
            r1 = [self._by_name[n] for n in s1 - s2]  # only in self
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

    def apply(self, schema: schemas.AbstractSchema, restrictions: Restrictions, access, transaction, ) -> schemas.AbstractSchema:
        return schema


class DataRestriction(SchemaRestriction):
    """ Restrictions on data for a schema """

    def apply(self, data: Tsubject, restrictions: Restrictions, schema: schemas.AbstractSchema, access, transaction) -> Tsubject:
        return data


class MustReview(SchemaRestriction):
    by_roles: set[str] = set()

    def merge(self, other: MustReview) -> typing.Self | None:
        """ return the stronger between self and other restrictions, creating a new combined 
            restriction. Any role is stronger than when no roles are specified. 
        """
        r = super().merge(other)
        if r is not None:
            if r.by_roles != other.by_roles:
                r = copy.copy(r)
                if self.may_overwrite:
                    r.by_roles = other.by_roles
                else:
                    r.by_roles.update(other.by_roles)
        return r


class MustHaveSensitivites(SchemaRestriction):
    """ This schema must have sensitivity annotations """
    ...


class MustValidate(DataRestriction):
    """ Data must be validated """

    def apply(self, data: Tsubject, restrictions: Restrictions, schema: schemas.AbstractSchema, access, transaction) -> Tsubject:
        remainder = access.ddhkey.remainder(access.schema_key_split)
        try:
            data = schema.validate_data(data, remainder, no_extra=NoExtraElements in restrictions)
        except Exception as e:
            raise errors.ValidationError(e)
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
