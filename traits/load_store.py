""" Executable Schema Transformers """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services


class AccessTransformer(trait.Transformer):
    """ Transformers on data for a schema """
    supports_modes = frozenset()  # Transformer is not invoked by mode
    only_modes = {permissions.AccessMode.write}  # no checks for read
    phase = trait.Phase.validation


class LoadFromStorage(AccessTransformer):
    """ Load data from storage """
    phase = trait.Phase.load
    only_modes = {permissions.AccessMode.read}
    only_forks = {keys.ForkType.data, keys.ForkType.consents}

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:
        data_node, d_key_split = keydirectory.NodeRegistry.get_node(
            access.ddhkey, nodes.NodeSupports.data, transaction)
        q = None
        if data_node:
            if access.ddhkey.fork == keys.ForkType.consents:
                access.include_mode(permissions.AccessMode.read)
                *d, consentees, msg = access.raise_if_not_permitted(data_node)
                return data_node.consents
            else:
                data_node = data_node.ensure_loaded(transaction)
                data_node = typing.cast(nodes.DataNode, data_node)
                *d, consentees, msg = access.raise_if_not_permitted(data_node)
                data = data_node.execute(nodes.Ops.get, access, transaction, d_key_split, None, q)
        else:
            *d, consentees, msg = access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
                access.ddhkey, nodes.NodeSupports.data, None, transaction))
            data = {}
        transaction.add_read_consentees({c.id for c in consentees})

        return data


class LoadFromDApp(AccessTransformer):
    """ Load data, passing it through DApp.
        TODO: Obtained data is raw JSON, not schemaed JSON (e.g., datetime remains str) 
    """
    phase = trait.Phase.load
    after = 'LoadFromStorage'
    only_modes = {permissions.AccessMode.read}
    only_forks = {keys.ForkType.data}  # consents and schemas are never loaded through apps

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:
        q = None
        e_node, e_key_split = keydirectory.NodeRegistry.get_node(
            access.ddhkey.without_owner(), nodes.NodeSupports.execute, transaction)
        if e_node:
            e_node = e_node.ensure_loaded(transaction)
            e_node = typing.cast(nodes.ExecutableNode, e_node)
            req = dapp_attrs.ExecuteRequest(
                op=nodes.Ops.get, access=access, transaction=transaction, key_split=e_key_split, data=data, q=q)
            data = await e_node.execute(req)
        return data


class ValidateToDApp(AccessTransformer):
    """ Validated Data to be saved by passing it to DApp """

    phase = trait.Phase.store  # this is actually the store phase, as it must have passed all of Phase.validation
    only_modes = {permissions.AccessMode.write}
    only_forks = {keys.ForkType.data}  # consents and schemas are never loaded through apps

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:
        return data


class SaveToStorage(AccessTransformer):
    """ Save data to storage """
    phase = trait.Phase.store
    after = 'ValidateToDApp'
    only_modes = {permissions.AccessMode.write}
    only_forks = {keys.ForkType.data, keys.ForkType.consents}

    async def apply(self,  traits: trait.Traits, schema, access, transaction, data: trait.Tsubject, **kw) -> trait.Tsubject:
        if data is None:
            #  nothing to store, perhaps ValidateToDApp stored everything
            return None
        else:
            # find DApp
            user = transaction.for_user  # TODO: Should be onwer of ressource?
            dapp = user.profile.system_services.get_dapp(system_services.SystemServices.storage)
            print('Dapp', dapp)

            # dapp.execute()

            ...
        return data


# Root Tranformers may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    LoadFromStorage(may_overwrite=True), LoadFromDApp(may_overwrite=True), ValidateToDApp(may_overwrite=True), SaveToStorage(may_overwrite=True))
