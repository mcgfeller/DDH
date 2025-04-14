""" Executable Schema Transformers """
from __future__ import annotations

import typing
import copy
import pydantic
from utils.pydantic_utils import CV

from core import (errors, trait, permissions, keys, nodes, data_nodes, executable_nodes, events,
                  keydirectory, dapp_attrs, transactions, common_ids, principals, consentcache)
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
            # there is no top node, create it if owner asks for it:
            if create:
                if trstate.access.principal.id in topkey.owner:
                    data_node = data_nodes.DataNode(owner=trstate.access.principal, key=topkey)
                    keyvault.set_new_storage_key(data_node, trstate.access.principal, set(), set())
                    # Add it to transaction:
                    trstate.transaction.add(persistable.UserDataPersistAction(obj=data_node))

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

            data = None  # we'll check this later in VerifyLoaded

        owner_ids = {o.id for o in data_node.owners} if data_node else set(trstate.access.ddhkey.owners)
        consentee_ids = {c.id for c in consentees}
        trstate.transaction.trx_ext['ConsenteesChecker'].add_read_consentees(trstate.transaction,
                                                                             owner_ids | consentee_ids, trstate.access.modes)
        trstate.parsed_data = data
        trstate.access.data_key_split = d_key_split

        return


class ConsenteesChecker(transactions.TrxExtension):
    read_consentees: set[common_ids.PrincipalId] = set()
    # same as read_consentees, set by .reinit() and not modified during transaction
    initial_read_consentees:  frozenset[common_ids.PrincipalId] = frozenset()

    def reinit(self):
        """ Consentees from previuos trx are recorded as initial, so we can check wether re-init is needed.
            They remain in trx, so the overall check is easier. 
        """
        self.initial_read_consentees = frozenset(self.read_consentees)

    @classmethod
    def class_init(cls, trx_class: type[transactions.Transaction]):
        """ For convenience, we let trx.read_consentees refer to the value maintained in our extension.
            This is badass Python.    
        """
        trx_class.read_consentees = property(lambda self: self.trx_ext['ConsenteesChecker'].read_consentees)

    def add_read_consentees(self, trx: transactions.Transaction, read_consentees: set[common_ids.PrincipalId], modes: set[permissions.AccessMode]):
        # record if it cannot be combined or is not shared with everybody:
        if permissions.AccessMode.combined not in modes and principals.AllPrincipal.id not in read_consentees:
            read_consentees.discard(trx.owner.id)  # remove ourselves, as we have consented access
            assert principals.AllPrincipal.id not in self.read_consentees
            if self.read_consentees:
                common = self.read_consentees & read_consentees
                if not common:
                    msg = f'transaction already contains data shared with {
                        self.read_consentees} that cannot be combined with data shared with {read_consentees}'
                    # already present in initial trx?
                    if self.initial_read_consentees and self.initial_read_consentees.isdisjoint(read_consentees):
                        # this transaction contains data from previous transaction, must reinit
                        raise transactions.SessionReinitRequired('call session.reinit(); '+msg)
                    else:
                        raise transactions.TrxAccessError(msg)
                else:
                    self.read_consentees = common
            else:
                self.read_consentees = read_consentees  # first set
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
            e_node = typing.cast(executable_nodes.ExecutableNode, e_node)
            req = dapp_attrs.ExecuteRequest(
                op=nodes.Ops.get, access=trstate.access, transaction=trstate.transaction, key_split=e_key_split, data=trstate.parsed_data, q=q)
            data = await e_node.execute(req)
            trstate.parsed_data = data
            trstate.access.data_key_split = e_key_split
        return


class VerifyLoaded(AccessTransformer):
    """ Check if data was loaded by LoadFromStorage or LoadFromDApp.
        We need this as we cannot know beforehand which Transformer might load data.
    """
    phase: CV[trait.Phase] = trait.Phase.load
    after: str = 'LoadFromDApp'
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.read})
    # consents and schemas are never loaded through apps
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        if trstate.parsed_data is None:
            top, remainder = trstate.access.ddhkey.split_at(trstate.access.data_key_split)
            raise errors.NotFound(remainder)
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
            e_node = typing.cast(executable_nodes.ExecutableNode, e_node)
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

        if trstate.data_node:
            remainder = trstate.access.ddhkey.split_at(trstate.access.data_key_split)
        else:
            data_node, d_key_split, remainder = await self.get_or_create_dnode(trstate, create=True)
            assert trstate.data_node

        remainder = remainder.without_variant_version()  # consent is independent of VV

        trstate.data_node = typing.cast(data_nodes.DataNode, trstate.data_node)
        assert trstate.data_node.key

        trstate.access.raise_if_not_permitted(trstate.data_node)

        key_affected, added, removed = await trstate.data_node.update_consents(trstate.access, trstate.transaction, remainder, trstate.parsed_data)
        if key_affected:
            trstate.transaction.add(persistable.UserDataPersistAction(obj=trstate.data_node, add_to_dir=False))
            newgrants = await consentcache.ConsentCache.update(key_affected, added, removed)
            # publish event for new consents:
            for principal, grants_added in newgrants.items():
                ev = events.ConsentEvent.for_principal(principal=principal, grants_added=set(grants_added))
                await ev.publish(trstate.transaction)
            # TODO: Add entry to pseudonym map
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
                d_key_split = 2  # top personal node
                topkey, remainder = access.ddhkey.split_at(d_key_split)
                # there is no node, create it if owner asks for it:
                if access.principal.id in topkey.owner:
                    data_node = data_nodes.DataNode(owner=access.principal, key=topkey)
                else:  # not owner, we simply say no access to this path
                    raise errors.AccessError(f'User {access.principal.id} not authorized to write to {topkey}')
            else:
                data_node = await data_node.ensure_loaded(transaction)
                topkey, remainder = access.ddhkey.split_at(d_key_split)

            data_node = typing.cast(data_nodes.DataNode, data_node)
            # Insert data into data_node:
            await data_node.execute(nodes.Ops.put, access, transaction, d_key_split, trstate.parsed_data)

            trstate.data_node = data_node  # new Node, add to transaction
            transaction.add(persistable.UserDataPersistAction(obj=data_node))
        return


class PublishEvent(AccessTransformer):
    """ Publish a change event """
    phase: CV[trait.Phase] = trait.Phase.store
    after: str = 'SaveToStorage'
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data, keys.ForkType.consents})

    async def apply(self,  traits: trait.Traits, trstate: trait.TransformerState, **kw):
        print('publish event for', trstate.access.ddhkey)
        ev = events.UpdateEvent(key=trstate.access.ddhkey)
        await ev.publish(trstate.transaction)
        return


# Root Tranformers may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(
    LoadFromStorage(may_overwrite=True), LoadFromDApp(may_overwrite=True), VerifyLoaded(may_overwrite=True), ValidateToDApp(may_overwrite=True),  UpdateConsents(), SaveToStorage(may_overwrite=True), PublishEvent(may_overwrite=True),)
