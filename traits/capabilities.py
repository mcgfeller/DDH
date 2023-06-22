""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key

from core import (errors, versions, permissions, schemas, transactions, trait, keys)
from backend import persistable

DataCapability = typing.ForwardRef('DataCapability')


class DataCapability(trait.Transformer):
    """ Capability used for Schemas """
    supports_modes = frozenset()
    only_forks = {keys.ForkType.data}

    async def apply(self, schema, access, transaction, data_by_principal: dict):
        return data_by_principal  # TODO: Check method in superclass
