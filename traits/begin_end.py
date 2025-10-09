""" Begin/End Transformers """


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

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):

        return


class FinalTransformer(BracketTransformer):
    """ Final transformer in chain """
    phase: CV[trait.Phase] = trait.Phase.last

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        self.audit(trstate.access, trstate.transaction)
        # print(f'FinalTransformer: {trstate.transaction!s}')
        await trstate.transaction.commit()
        return


class AbortTransformer(BracketTransformer):
    """ Special transformer called only if other transformers cause exceptions """
    phase: CV[trait.Phase] = trait.Phase.none_

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, failing: trait.Transformer, exception: Exception, **kw):
        await trstate.transaction.abort()
        # Record failure and commit audit log
        trstate.access.failed = str(exception)
        self.audit(trstate.access, trstate.transaction)
        await trstate.transaction.commit()
        return


class QueryParamTransformer(BracketTransformer):
    """ Validate QueryParams, as specified in trait.QueryParams """
    phase: CV[trait.Phase] = trait.Phase.first

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        """ Obtain QueryParamCls according to schema_attributes.query_params_class,
            then parse trstate.raw_query_params into trstate.query_params.
        """
        classname = trstate.nschema.schema_attributes.query_params_class
        QueryParamCls = trait.QueryParams.get_class(classname)
        if trstate.raw_query_params:
            try:
                params = QueryParamCls.model_validate(trstate.raw_query_params)
            except pydantic.ValidationError as e:
                raise errors.ValidationError(e)
        else:  # default
            params = QueryParamCls()
        # print(f'QueryParamTransformer: {classname=}, {params=}')
        trstate.query_params = params

        return


# Root Tranformers may not be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    BeginTransformer(may_overwrite=False), QueryParamTransformer(may_overwrite=False), FinalTransformer(may_overwrite=False))
trait.DefaultTraits._AbortTransformer = trait.Transformers(AbortTransformer())
