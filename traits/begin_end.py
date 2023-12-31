""" Begin/End Transformers """
from __future__ import annotations

import typing
import copy
import pydantic
from utils.pydantic_utils import CV
from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services, persistable, audit


class BracketTransformer(trait.Transformer):
    """ Transformers at begin or end """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # Transformer is not invoked by mode
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # no restrictons
    only_forks: CV[frozenset[keys.ForkType]] = frozenset()

    def audit(self, access, transaction):
        """ Create AuditRecord and add it to transaction. """
        apa = audit.AuditPersistAction(obj=audit.AuditRecord.from_access(access))
        transaction.add(apa)


class BeginTransformer(BracketTransformer):
    """ First transformer in chain """
    phase: CV[trait.Phase] = trait.Phase.first

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):

        return


class FinalTransformer(BracketTransformer):
    """ Final transformer in chain """
    phase: CV[trait.Phase] = pydantic.Field(
        default=trait.Phase.last, description="phase in which transformer executes, for ordering.")

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        self.audit(trargs.access, trargs.transaction)
        print(f'FinalTransformer: {trargs.transaction!s}')
        await trargs.transaction.commit()
        return


class AbortTransformer(BracketTransformer):
    """ Special transformer called only if other transformers cause exceptions """
    phase: CV[trait.Phase] = pydantic.Field(
        default=trait.Phase.none_, description="phase in which transformer executes, for ordering.")

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, failing: trait.Transformer, exception: Exception, **kw):
        await trargs.transaction.abort()
        # Record failure and commit audit log
        trargs.access.failed = str(exception)
        self.audit(trargs.access, trargs.transaction)
        await trargs.transaction.commit()
        return


# Root Tranformers may not be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    BeginTransformer(may_overwrite=False), FinalTransformer(may_overwrite=False))
trait.DefaultTraits._AbortTransformer = trait.Transformers(AbortTransformer())
