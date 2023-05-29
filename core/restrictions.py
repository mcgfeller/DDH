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

from . import (errors, versions, permissions, schemas, transactions, assignable)

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Restriction(assignable.Assignable):

    def apply(self, subject: Tsubject, restrictions: Restrictions, *a, **kw) -> Tsubject:
        return subject


SchemaRestriction = typing.ForwardRef('SchemaRestriction')
Restrictions = typing.ForwardRef('Restrictions')


class Restrictions(assignable.Assignables):
    """ A collection of Restriction.
        Restriction is not hashable, so we keep a list and build a dict with the class names. 
    """

    def apply(self, restriction_class: type[Restriction], subject: Tsubject, *a, **kw) -> Tsubject:
        """ apply restrictions in turn """
        for restriction in self.assignables:
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
        for owner, d in data.items():  # loop through owners, as schema doesn't contain owner
            try:
                data[owner] = schema.validate_data(d, remainder, no_extra=NoExtraElements in restrictions)
            except Exception as e:
                raise errors.ValidationError(e)

        return data


class NoExtraElements(DataRestriction):
    """ Schema validation will reject extra elements not specified in the schema;
        marker applied by MustValidate
    """
    ...


class LatestVersion(DataRestriction):
    """ Data must match latest version of schema or must be upgradable.
    """
    ...


NoRestrictions = Restrictions()
# Root restrictions may be overwritten:
RootRestrictions = Restrictions(MustValidate(may_overwrite=True), NoExtraElements(may_overwrite=True))
NoValidation = Restrictions(~MustValidate(may_overwrite=True), ~NoExtraElements(may_overwrite=True))
HighPrivacyRestrictions = Restrictions(MustValidate(), NoExtraElements(), MustHaveSensitivites(), MustReview())
# Ensure we have a senior reviewer:
HighestPrivacyRestrictions = HighPrivacyRestrictions+MustReview(by_roles={'senior'})
