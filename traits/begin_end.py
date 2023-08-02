""" Begin/End Transformers """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services, persistable, audit


class BracketTransformer(trait.Transformer):
    """ Transformers at begin or end """
    supports_modes = frozenset()  # Transformer is not invoked by mode
    only_modes = frozenset()  # no restrictons
    only_forks = frozenset()

    def audit(self, access, transaction):
        """ Create AuditRecord and add it to transaction. """
        apa = audit.AuditPersistAction(obj=audit.AuditRecord.from_access(access))
        transaction.add(apa)


class BeginTransformer(BracketTransformer):
    """ First transformer in chain """
    phase = trait.Phase.first

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):

        return


class FinalTransformer(BracketTransformer):
    """ Final transformer in chain """
    phase = trait.Phase.last

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        self.audit(trargs.access, trargs.transaction)
        print(f'FinalTransformer: {trargs.transaction!s}')
        await trargs.transaction.commit()
        return


class AbortTransformer(BracketTransformer):
    """ Special transformer called only if other transformers cause exceptions """
    phase = trait.Phase.none_

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
