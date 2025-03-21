
""" DDH DataNode """
from __future__ import annotations
import typing


from . import permissions, transactions, errors, keydirectory, users, common_ids, nodes, keys, dapp_proxy, storage_resource, principals
from utils import datautils
from backend import persistable, system_services, storage, keyvault


class DataNode(nodes.Node, persistable.Persistable):
    """ New data node, points to storage and consents """

    data: typing.Any = None
    format: persistable.DataFormat = persistable.DataFormat.dict
    storage_dapp_id: str | None = None
    access_key: keyvault.AccessKey | None = None
    sub_nodes: dict[keys.DDHkey, keys.DDHkey] = {}

    @classmethod
    def get_storage_dapp_id(cls, owner: principals.Principal) -> str:
        assert owner
        profile = getattr(owner, 'profile', users.DefaultProfile)  # Test principals don't have profile
        dappid = profile.system_services.system_dapps.get(system_services.SystemServices.storage)
        assert dappid
        return dappid

    @property
    def supports(self) -> set[nodes.NodeSupports]:
        s = {nodes.NodeSupports.data}
        if self.consents:
            s.add(nodes.NodeSupports.consents)
        return s

    def all_accessors(self) -> set[principals.Principal]:
        """ return set of all principals that access this node and hence needs keys """
        r = set(self.owners)
        if self.consents:  # add consentees
            r = r | self.consents.consentees()
        return r

    @classmethod
    async def get_storage_resource(cls, owner: principals.Principal,  transaction: transactions.Transaction) -> storage_resource.StorageResource:
        da = cls.get_storage_dapp_id(owner)
        res = transaction.resources.get(da)
        if not res:
            res = storage_resource.StorageResource.create(da)
            await transaction.add_resource(res)
        assert isinstance(res, storage_resource.StorageResource)
        return res

    async def store(self, transaction: transactions.Transaction):
        """ Store then node on encrypted storage.
            Does not add it to the directory, use .ensure_in_dir() for this. 
        """
        res = await self.get_storage_resource(self.owner, transaction)
        d = self.to_compressed()
        if self.id not in storage.Storage:
            # we need a storage key first:
            keyvault.set_new_storage_key(self, transaction.owner, self.all_accessors(), set())
        enc = keyvault.encrypt_data(transaction.owner, self.id, d)
        await res.store(self.id, enc, transaction)
        return

    def ensure_in_dir(self, key, transaction: transactions.Transaction):
        """ Add this Node to the registry unless it is there already. """
        keydirectory.NodeRegistry.check_and_set(key, self)

    async def delete(self, transaction: transactions.Transaction):
        await self.__class__.load(self.id, self.owner, transaction)  # verify encryption by loading
        res = await self.get_storage_resource(self.owner, transaction)
        await res.delete(self.id, transaction)
        return

    @classmethod
    async def load(cls, id: common_ids.PersistId, owner: principals.Principal,  transaction: transactions.Transaction):
        res = await cls.get_storage_resource(owner, transaction)
        enc = await res.load(id, transaction)
        # enc = storage.Storage.load(id, transaction)
        try:
            plain = keyvault.decrypt_data(transaction.owner, id, enc)
        except KeyError as e:  # there is no entry for the user in keyvault.PrincipalKeyVault, so we cannot load this node
            raise errors.AccessError(f'User {transaction.owner.id} not authorized to load node {id}')
        o = cls.from_compressed(plain)
        assert o.key.key[0] is keys.DDHkey.Root
        return o

    async def execute(self, op: nodes.Ops, access: permissions.Access, transaction: transactions.Transaction, key_split: int, data: dict | None = None, q: str | None = None):
        if key_split:
            top, remainder = access.ddhkey.split_at(key_split)
            if self.format != persistable.DataFormat.dict:
                raise errors.NotSelectable(remainder)
        if op == nodes.Ops.get:
            data = await self.unsplit_data(self.data, transaction)
            if key_split:
                # we tolerate not found here and check in VerifyLoaded transformer:
                data = datautils.extract_data(self.data, remainder, default=None)
        elif op == nodes.Ops.put:
            assert data is not None
            if key_split:
                self.data = datautils.insert_data(self.data or {}, remainder, data, missing=dict)
            await self.store(transaction)
        elif op == nodes.Ops.delete:
            await self.delete(transaction)

        return data

    async def update_consents(self, access: permissions.Access, transaction: transactions.Transaction, remainder: keys.DDHkeyGeneric, consents: permissions.Consents) -> tuple[keys.DDHkey | None, frozenset[permissions.Consent], frozenset[permissions.Consent]]:
        """ update consents at remainder key.
            Data must be read using previous encryption and rewritten using the new encryption. See 
            section 7.3 "Protection of data at rest and on the move" of the DDH paper.
        """
        assert self.key
        eff_principals = consents.consentees()
        if self.consents:  # had consents before, check changes:
            added, removed = self.consents.changes(consents)
            # existing principals now longer effective - must be deleted:
            del_principals = self.consents.consentees() - eff_principals
        else:  # all new
            added = consents.consents; removed = frozenset()
            del_principals = set()  # all new, nobody to remove

        key_affected = None
        if added or removed:  # expensive op follows, do only if something has changed
            await self.ensure_loaded(transaction)  # we must ensure data is read
            self.consents = consents  # actually update

            if remainder.key:  # change is not at this level, insert a new node:
                node = self.split_node(remainder, consents)
                # Record the hole with reference to the below-node. We keep the below node in the directory, so
                # its data can be accessed without access to self:
                node.ensure_in_dir(node.key, transaction)
            else:
                above = None
                node = self  # top level

            keyvault.set_new_storage_key(node, access.principal, eff_principals,
                                         del_principals)  # now we can set the new key

            # re-encrypt on new node (may be self if there is no remainder)
            await node.store(transaction)
            # key is node key, but never consents fork, and without variant and version:
            assert node.key
            key_affected = node.key.for_consent_grants()

            if remainder.key:  # need to write data with below part cut out again, but with changed key
                await self.store(transaction)  # old node

        return key_affected, added, removed

    def split_node(self, remainder: keys.DDHkey, consents: permissions.Consents) -> DataNode:
        prev_data = self.data  # need before new key is generated
        if self.format != persistable.DataFormat.dict:
            raise errors.NotSelectable(remainder)
        key = keys.DDHkey(key=self.key.key+remainder.key)
        # if prev_data is not complete until remainder, fill it:
        if datautils.hole is datautils.extract_data(prev_data, remainder, default=datautils.hole):
            prev_data = datautils.insert_data(prev_data or {}, remainder, None, missing=dict)
        above, below = datautils.split_data(
            prev_data, remainder, raise_error=errors.NotFound)  # if we're deep in data
        node = self.__class__(owner=self.owner, key=key, consents=consents, data=below)
        self.data = above
        return node

    async def unsplit_data(self, data, transaction):
        for remainder, fullkey in self.sub_nodes.items():
            subnodeproxy = keydirectory.NodeRegistry[fullkey][nodes.NodeSupports.data]
            if subnodeproxy:
                subnode = await subnodeproxy.ensure_loaded(transaction)
                assert isinstance(subnode, DataNode)  # because of lookup by type
                data = datautils.insert_data(data, remainder, subnode.data)

        return data


DataNode.model_rebuild()  # Now Node is known, update before it's derived
