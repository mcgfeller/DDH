""" Executable Schema Transformers """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from backend import system_services, persistable


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

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        data_node, d_key_split = keydirectory.NodeRegistry.get_node(
            trargs.access.ddhkey, nodes.NodeSupports.data, trargs.transaction)
        q = None
        if data_node:
            if trargs.access.ddhkey.fork == keys.ForkType.consents:
                trargs.access.include_mode(permissions.AccessMode.read)
                *d, consentees, msg = trargs.access.raise_if_not_permitted(data_node)
                return data_node.consents
            else:
                data_node = data_node.ensure_loaded(trargs.transaction)
                data_node = typing.cast(nodes.DataNode, data_node)
                *d, consentees, msg = trargs.access.raise_if_not_permitted(data_node)
                data = data_node.execute(nodes.Ops.get, trargs.access, trargs.transaction, d_key_split, None, q)
            trargs.data_node = data_node
        else:
            *d, consentees, msg = trargs.access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
                trargs.access.ddhkey, nodes.NodeSupports.data, None, trargs.transaction))
            data = {}
        trargs.transaction.add_read_consentees({c.id for c in consentees})
        trargs.parsed_data = data

        return


class LoadFromDApp(AccessTransformer):
    """ Load data, passing it through DApp.
        TODO: Obtained data is raw JSON, not schemaed JSON (e.g., datetime remains str) 
    """
    phase = trait.Phase.load
    after = 'LoadFromStorage'
    only_modes = {permissions.AccessMode.read}
    only_forks = {keys.ForkType.data}  # consents and schemas are never loaded through apps

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        q = None
        e_node, e_key_split = keydirectory.NodeRegistry.get_node(
            trargs.access.ddhkey.without_owner(), nodes.NodeSupports.execute, trargs.transaction)
        if e_node:
            e_node = e_node.ensure_loaded(trargs.transaction)
            e_node = typing.cast(nodes.ExecutableNode, e_node)
            req = dapp_attrs.ExecuteRequest(
                op=nodes.Ops.get, access=trargs.access, transaction=trargs.transaction, key_split=e_key_split, data=trargs.parsed_data, q=q)
            data = await e_node.execute(req)
            trargs.parsed_data = data
        return


class ValidateToDApp(AccessTransformer):
    """ Validated Data to be saved by passing it to DApp """

    phase = trait.Phase.store  # this is actually the store phase, as it must have passed all of Phase.validation
    only_modes = {permissions.AccessMode.write}
    only_forks = {keys.ForkType.data}  # consents and schemas are never loaded through apps

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        return


class SaveToStorage(AccessTransformer):
    """ Save data to storage """
    phase = trait.Phase.store
    after = 'ValidateToDApp'
    only_modes = {permissions.AccessMode.write}
    only_forks = {keys.ForkType.data, keys.ForkType.consents}

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        if trargs.parsed_data is None:
            #  nothing to store, perhaps ValidateToDApp stored everything
            return None
        else:
            access = trargs.access
            transaction = trargs.transaction

            data_node, d_key_split = keydirectory.NodeRegistry.get_node(
                access.ddhkey, nodes.NodeSupports.data, transaction)
            if not data_node:

                topkey, remainder = access.ddhkey.split_at(2)
                # there is no node, create it if owner asks for it:
                if access.principal.id in topkey.owners:
                    data_node = nodes.DataNode(owner=access.principal, key=topkey)
                    # data_node.store(transaction)  # XXX? # put node into directory
                else:  # not owner, we simply say no access to this path
                    raise errors.AccessError(f'User {access.principal.id} not authorized to write to {topkey}')
            else:
                data_node = data_node.ensure_loaded(transaction)
                topkey, remainder = access.ddhkey.split_at(d_key_split)

            data_node = typing.cast(nodes.DataNode, data_node)
            # TODO: Insert data into data_node

            trargs.data_node = data_node  # TODO NEW NODE!
            # Add it to transaction:
            transaction.add(persistable.UserDataPersistAction(obj=data_node))
        return


# Root Tranformers may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    LoadFromStorage(may_overwrite=True), LoadFromDApp(may_overwrite=True), ValidateToDApp(may_overwrite=True), SaveToStorage(may_overwrite=True))
