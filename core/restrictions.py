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
    ...


SchemaRestriction = typing.ForwardRef('SchemaRestriction')
Restrictions = typing.ForwardRef('Restrictions')


class Restrictions(DDHbaseModel):
    """ A collection of Restriction """
    restrictions: list[Restriction] = []

    def merge(self, other: Restrictions) -> typing.Self:
        """ return the stronger of self and other restrictions, creating a new combined 
            restrictions.
        """
        return self


class SchemaRestriction(Restriction):
    """ Restriction used for Schemas """

    may_overwrite: bool = pydantic.Field(
        default=False, description="restriction may be overwritten explicitly in lower schema")

    def apply(self, schema: schemas.AbstractSchema, access, transaction, data):
        return data


class MustReview(SchemaRestriction):
    by_roles: set[str] = set()


class MustHaveSensitivites(SchemaRestriction):
    """ This schema must have sensitivity annotations """
    ...


DefaultRestrictions = Restrictions()
HighPrivacyRestrictions = Restrictions(restrictions=[MustReview(), MustHaveSensitivites()])
HighestPrivacyRestrictions = Restrictions(restrictions=[MustReview(by_roles={'senior'}), MustHaveSensitivites()])
