""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key

from core import (errors, versions, permissions, schemas, transactions, assignable)
from backend import persistable

SchemaCapability = typing.ForwardRef('SchemaCapability')


class SchemaCapability(assignable.Applicable):
    """ Capability used for Schemas """
    supports_modes = frozenset()

    def apply(self, schema, access, transaction, data_by_principal: dict):
        return data_by_principal  # TODO: Check method in superclass
