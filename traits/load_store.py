""" Executable Schema Transformers """
from __future__ import annotations

import typing
import copy
import pydantic
from utils.pydantic_utils import CV

from core import (errors, trait, permissions, keys, nodes, data_nodes, keydirectory, dapp_attrs)
from backend import persistable, keyvault


class AccessTransformer(trait.Transformer):
    """ Transformers on data for a schema """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # Transformer is not invoked by mode
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})  # no checks for read
    phase: CV[trait.Phase] = trait.Phase.validation

    async def get_or_create_dnode(self, trstate: trait.TransformerState, create: bool = False, condition: typing.Callable | None = None) -> tuple[data_nodes.DataNode | None, int, keys.DDHkey]:
        if create and condition:
            raise ValueError('create and condition may not be set simultaneously')
        try:
            data_node, d_key_split = await keydirectory.NodeRegistry.get_node_async(
                trstate.access.ddhkey, nodes.NodeSupports.data, trstate.transaction, condition=condition)
        except errors.AccessError as e:
            raise errors.AccessError(
                f'User {trstate.access.principal.id} not authorized to read {trstate.access.ddhkey}')

        if data_node:
            data_node = typing.cast(persistable.SupportsLoading, data_node)  # we can apply ensure_loaded
            data_node = await data_node.ensure_loaded(trstate.transaction)
            data_node = typing.cast(data_nodes.DataNode, data_node)  # now we know it's a DataNode
            topkey, remainder = trstate.access.ddhkey.split_at(d_key_split)
        else:
            topkey, remainder = trstate.access.ddhkey.split_at(2)
            # there is no node, create it if owner asks for it:
            if create:
                if trstate.access.principal.id in topkey.owner:
                    data_node = data_nodes.DataNode(owner=trstate.access.principal, key=topkey)
                    keyvault.set_new_storage_key(data_node, trstate.access.principal, set(), set())
                    await data_node.store(trstate.transaction)  #
                    data_node.ensure_in_dir(data_node.key, trstate.transaction)  # put node into directory

                else:  # not owner, we simply say no access to this path
                    raise errors.AccessError(f'User {trstate.access.principal.id} not authorized to write to {topkey}')

        trstate.data_node = data_node

        return data_node, d_key_split, remainder


class LoadFromStorage(AccessTransformer):
    """ Load data from storage """
    phase: CV[trait.Phase] = trait.Phase.load
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.read})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data, keys.ForkType.consents})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        """ load from storage, per storage according to user profile """

        for_consents = trstate.access.ddhkey.fork == keys.ForkType.consents
        data_node, d_key_split, remainder = await self.get_or_create_dnode(trstate, create=for_consents)
        q = None

        if data_node:
            if trstate.access.ddhkey.fork == keys.ForkType.consents:
                trstate.access.include_mode(permissions.AccessMode.read)
                *d, consentees, msg = trstate.access.raise_if_not_permitted(data_node)
                consents = data_node.consents if data_node.consents else permissions.DefaultConsents
                data = consents.model_dump()
            else:
                *d, consentees, msg = trstate.access.raise_if_not_permitted(data_node)
                data = await data_node.execute(nodes.Ops.get, trstate.access, trstate.transaction, d_key_split, None, q)
            trstate.data_node = data_node
        else:  # we have no data_node, but need a consent node to check whether we can read here:
            data_node, d_key_split, remainder = await self.get_or_create_dnode(trstate, condition=nodes.Node.has_consents)
            *d, consentees, msg = trstate.access.raise_if_not_permitted(data_node)

            data = {}

        owner_ids = {o.id for o in data_node.owners} if data_node else set(trstate.access.ddhkey.owners)
        consentee_ids = {c.id for c in consentees}
        trstate.transaction.add_read_consentees(owner_ids | consentee_ids, trstate.access.modes)
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


class UpdateConsents(AccessTransformer):
    """ Upate consents with validated data """
    phase: CV[trait.Phase] = trait.Phase.validation
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.consents})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):

        # validate new consents first:
        trstate.parsed_data = permissions.Consents.model_validate_json(trstate.orig_data)

        if not trstate.data_node:
            data_node, d_key_split, remainder = await self.get_or_create_dnode(trstate, create=True)
            assert trstate.data_node
        trstate.data_node = typing.cast(data_nodes.DataNode, trstate.data_node)

        trstate.access.raise_if_not_permitted(trstate.data_node)

        await trstate.data_node.update_consents(trstate.access, trstate.transaction, remainder, trstate.parsed_data)
        await trstate.data_node.store(trstate.transaction)
        return


class SaveToStorage(AccessTransformer):
    """ Save data to storage """
    phase: CV[trait.Phase] = trait.Phase.store
    after: str = 'ValidateToDApp'
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data, })

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
                    await data_node.store(transaction)  # XXX? # put node into directory
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
    LoadFromStorage(may_overwrite=True), LoadFromDApp(may_overwrite=True), ValidateToDApp(may_overwrite=True),  UpdateConsents(), SaveToStorage(may_overwrite=True))
