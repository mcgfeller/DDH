""" Begin/End Transformers """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services, persistable


class BracketTransformer(trait.Transformer):
    """ Transformers at begin or end """
    supports_modes = frozenset()  # Transformer is not invoked by mode
    only_modes = frozenset()  # no restrictons
    only_forks = frozenset()

    def audit(self, access, transaction):
        apa = persistable.AuditPersistAction(obj=access.audit_record())
        transaction.add(apa)


class BeginTransformer(BracketTransformer):
    """ First transformer in chain """
    phase = trait.Phase.first

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:

        return data


class FinalTransformer(BracketTransformer):
    """ Final transformer in chain """
    phase = trait.Phase.last

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:
        self.audit(access, transaction)
        await transaction.commit()
        return data


class AbortTransformer(BracketTransformer):
    """ Special transformer called only if other transformers cause exceptions """
    phase = trait.Phase.none_

    async def apply(self,  traits: trait.Traits, schema, access: permissions.Access, transaction, data: trait.Tsubject, failing: trait.Transformer, exception: Exception, **kw) -> trait.Tsubject:
        await transaction.abort()
        # Record failure and commit audit log
        access.failed = str(exception)
        self.audit(access, transaction)
        await transaction.commit()
        return data


# Root Tranformers may not be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    BeginTransformer(may_overwrite=False), FinalTransformer(may_overwrite=False))
trait.DefaultTraits._AbortTransformer = trait.Transformers(AbortTransformer())
