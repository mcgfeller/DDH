""" Begin/End Transformers """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services


class BracketTransformer(trait.Transformer):
    """ Transformers at begin or end """
    supports_modes = frozenset()  # Transformer is not invoked by mode
    only_modes = {}  # no restrictons
    only_forks = {}


class BeginTransformer(BracketTransformer):
    """ First transformer in chain """
    phase = trait.Phase.first

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:

        return data


class FinalTransformer(BracketTransformer):
    """ Final transformer in chain """
    phase = trait.Phase.last

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:

        return data


# Root Tranformers may not be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    BeginTransformer(may_overwrite=False), FinalTransformer(may_overwrite=False))
