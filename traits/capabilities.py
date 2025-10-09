""" Executable Capabilities, especially for Schemas """


import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, CV, tuple_key_to_str, str_to_tuple_key

from core import (errors, versions, permissions, schemas, transactions, trait, keys)
from backend import persistable

DataCapability = typing.ForwardRef('DataCapability')


class DataCapability(trait.Transformer):
    """ Capability used for Schemas """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        return  # TODO: Check method in superclass
