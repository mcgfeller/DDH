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
