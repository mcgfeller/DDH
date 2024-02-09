""" Executable Schema Transformers """
from __future__ import annotations

import typing
import copy
import pydantic
from utils.pydantic_utils import CV

from core import (errors, trait, permissions, keys, nodes, data_nodes, keydirectory, dapp_attrs)
from backend import persistable


class AccessTransformer(trait.Transformer):
    """ Transformers on data for a schema """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # Transformer is not invoked by mode
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})  # no checks for read
    phase: CV[trait.Phase] = trait.Phase.validation


class LoadFromStorage(AccessTransformer):
    """ Load data from storage """
    phase: CV[trait.Phase] = trait.Phase.load
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.read})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data, keys.ForkType.consents})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        """ load from storage, per storage according to user profile """
        data_node, d_key_split = await keydirectory.NodeRegistry.get_node_async(
            trstate.access.ddhkey, nodes.NodeSupports.data, trstate.transaction)
        q = None
        if data_node:
            if trstate.access.ddhkey.fork == keys.ForkType.consents:
                trstate.access.include_mode(permissions.AccessMode.read)
                *d, consentees, msg = trstate.access.raise_if_not_permitted(data_node)
                return data_node.consents
            else:
                data_node = await data_node.ensure_loaded(trstate.transaction)
                data_node = typing.cast(data_nodes.DataNode, data_node)
                *d, consentees, msg = trstate.access.raise_if_not_permitted(data_node)
                data = await data_node.execute(nodes.Ops.get, trstate.access, trstate.transaction, d_key_split, None, q)
            trstate.data_node = data_node
        else:
            *d, consentees, msg = trstate.access.raise_if_not_permitted(await keydirectory.NodeRegistry._get_consent_node_async(
                trstate.access.ddhkey, nodes.NodeSupports.data, None, trstate.transaction))
            data = {}
        trstate.transaction.add_read_consentees({c.id for c in consentees})
        trstate.parsed_data = data

        return


class LoadFromDApp(AccessTransformer):
    """ Load data, passing it through DApp.
        TODO: Obtained data is raw JSON, not schemaed JSON (e.g., datetime remains str) 
    """
    phase: CV[trait.Phase] = trait.Phase.load
    after: str = 'LoadFromStorage'
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.read})
    # consents and schemas are never loaded through apps
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        q = None
        e_node, e_key_split = await keydirectory.NodeRegistry.get_node_async(
            trstate.access.ddhkey.without_owner(), nodes.NodeSupports.execute, trstate.transaction)
        if e_node:
            e_node = await e_node.ensure_loaded(trstate.transaction)
            e_node = typing.cast(nodes.ExecutableNode, e_node)
            req = dapp_attrs.ExecuteRequest(
                op=nodes.Ops.get, access=trstate.access, transaction=trstate.transaction, key_split=e_key_split, data=trstate.parsed_data, q=q)
            data = await e_node.execute(req)
            trstate.parsed_data = data
        return


class ValidateToDApp(AccessTransformer):
    """ Validated Data to be saved by passing it to DApp """

    # It must have passed all of Phase.validation, but not yet be depseudonymized
    phase: CV[trait.Phase] = trait.Phase.pre_store
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    # consents and schemas are never loaded through apps
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        # Call DApp
        q = None
        e_node, e_key_split = await keydirectory.NodeRegistry.get_node_async(
            trstate.access.ddhkey.without_owner(), nodes.NodeSupports.execute, trstate.transaction)
        if e_node:
            e_node = await e_node.ensure_loaded(trstate.transaction)
            e_node = typing.cast(nodes.ExecutableNode, e_node)
            req = dapp_attrs.ExecuteRequest(
                op=nodes.Ops.put, access=trstate.access, transaction=trstate.transaction, key_split=e_key_split, data=trstate.parsed_data, q=q)
            data = await e_node.execute(req)

            if data is not None:
                trstate.parsed_data = data
        return


class SaveToStorage(AccessTransformer):
    """ Save data to storage """
    phase: CV[trait.Phase] = trait.Phase.store
    after: str = 'ValidateToDApp'
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data, keys.ForkType.consents})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        if trstate.parsed_data is None:
            #  nothing to store, perhaps ValidateToDApp stored everything
            return None
        else:
            access = trstate.access
            transaction = trstate.transaction

            data_node, d_key_split = await keydirectory.NodeRegistry.get_node_async(
                access.ddhkey, nodes.NodeSupports.data, transaction)
            if not data_node:

                topkey, remainder = access.ddhkey.split_at(2)
                # there is no node, create it if owner asks for it:
                if access.principal.id in topkey.owner:
                    data_node = data_nodes.DataNode(owner=access.principal, key=topkey)
                    # data_node.store(transaction)  # XXX? # put node into directory
                else:  # not owner, we simply say no access to this path
                    raise errors.AccessError(f'User {access.principal.id} not authorized to write to {topkey}')
            else:
                data_node = await data_node.ensure_loaded(transaction)
                topkey, remainder = access.ddhkey.split_at(d_key_split)

            data_node = typing.cast(data_nodes.DataNode, data_node)
            # TODO: Insert data into data_node
            await data_node.execute(nodes.Ops.put, access, transaction, d_key_split, trstate.parsed_data)

            trstate.data_node = data_node  # TODO NEW NODE!
            # Add it to transaction:
            transaction.add(persistable.UserDataPersistAction(obj=data_node))
        return


# Root Tranformers may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    LoadFromStorage(may_overwrite=True), LoadFromDApp(may_overwrite=True), ValidateToDApp(may_overwrite=True), SaveToStorage(may_overwrite=True))
