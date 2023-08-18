
""" DDH DataNode """
from __future__ import annotations
import typing


from . import permissions, transactions, errors, keydirectory, users, common_ids, nodes, keys, dapp_proxy
from utils import datautils
from backend import persistable, system_services, storage, keyvault


class DataNode(nodes.Node, persistable.Persistable):
    """ New data node, points to storage and consents """

    format: persistable.DataFormat = persistable.DataFormat.dict
    data: typing.Any
    storage_dapp_id: str | None = None
    access_key: keyvault.AccessKey | None = None
    sub_nodes: dict[keys.DDHkey, keys.DDHkey] = {}

    def get_storage_dapp_id(self) -> str:
        assert self.owner
        profile = getattr(self.owner, 'profile', users.DefaultProfile)  # Test principals don't have profile
        dappid = profile.system_services.system_dapps.get(system_services.SystemServices.storage)
        assert dappid
        return dappid

    @property
    def supports(self) -> set[nodes.NodeSupports]:
        s = {nodes.NodeSupports.data}
        if self.consents:
            s.add(nodes.NodeSupports.consents)
        return s

    async def store(self, transaction: transactions.Transaction):
        da = self.get_storage_dapp_id()
        res = transaction.ressources.get(da)
        if not res:
            res = dapp_proxy.DAppRessource.create(da)  # TODO:#22
            await transaction.add_ressource(res)
        assert isinstance(res, dapp_proxy.DAppRessource)
        d = self.to_compressed()
        if self.id not in storage.Storage:
            keyvault.set_new_storage_key(self, transaction.for_user, set(), set())
        enc = keyvault.encrypt_data(transaction.for_user, self.id, d)
        await res.store(self.id, enc, transaction)
        # storage.Storage.store(self.id, enc, transaction)
        return

    @classmethod
    async def load(cls, id: common_ids.PersistId,  transaction: transactions.Transaction):
        da = 'InMemStorageDApp'  # XXX self.get_storage_dapp_id()
        res = transaction.ressources.get(da)
        if not res:
            res = dapp_proxy.DAppRessource.create(da)  # TODO:#22
            await transaction.add_ressource(res)
        assert isinstance(res, dapp_proxy.DAppRessource)

        enc = await res.load(id, transaction)
        # enc = storage.Storage.load(id, transaction)
        plain = keyvault.decrypt_data(transaction.for_user, id, enc)
        o = cls.from_compressed(plain)
        return o

    async def execute(self, op: nodes.Ops, access: permissions.Access, transaction: transactions.Transaction, key_split: int, data: dict | None = None, q: str | None = None):
        if key_split:
            top, remainder = access.ddhkey.split_at(key_split)
            if self.format != persistable.DataFormat.dict:
                raise errors.NotSelectable(remainder)
        if op == nodes.Ops.get:
            data = await self.unsplit_data(self.data, transaction)
            if key_split:
                data = datautils.extract_data(self.data, remainder, raise_error=errors.NotFound)
        elif op == nodes.Ops.put:
            assert data is not None
            if key_split:
                self.data = datautils.insert_data(self.data or {}, remainder, data, missing=dict)
            await self.store(transaction)
        elif op == nodes.Ops.delete:
            await self.delete(transaction)

        return data

    async def update_consents(self, access: permissions.Access, transaction: transactions.Transaction, remainder: keys.DDHkey, consents: permissions.Consents):
        """ update consents at remainder key.
            Data must be read using previous encryption and rewritten using the new encryption. See 
            section 7.3 "Protection of data at rest and on the move" of the DDH paper.
        """
        assert self.key
        if self.consents:  # had consents before, check changes:
            added, removed = self.consents.changes(consents)
            effective = consents.consentees()
        else:  # all new
            added = effective = consents.consentees(); removed = set()

        if added or removed:  # expensive op follows, do only if something has changed
            self.consents = consents  # actually update

            if remainder.key:  # change is not at this level, insert a new node:
                node = self.split_node(remainder, consents)
            else:
                above = None
                node = self  # top level

            keyvault.set_new_storage_key(node, access.principal, effective,
                                         removed)  # now we can set the new key

            # re-encrypt on new node (may be self if there is not remainder)
            await node.store(transaction)
            if remainder.key:  # need to write data with below part cut out again, but with changed key

                await self.store(transaction)  # old node

        return

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
        # Record the hole with reference to the below-node. We keep the below node in the directory, so
        # its data can be accessed without access to self.
        keydirectory.NodeRegistry[key] = node
        return node

    async def unsplit_data(self, data, transaction):
        for remainder, fullkey in self.sub_nodes.items():
            subnodeproxy = keydirectory.NodeRegistry[fullkey][nodes.NodeSupports.data]
            if subnodeproxy:
                subnode = await subnodeproxy.ensure_loaded(transaction)
                assert isinstance(subnode, DataNode)  # because of lookup by type
                data = datautils.insert_data(data, remainder, subnode.data)

        return data


DataNode.update_forward_refs()  # Now Node is known, update before it's derived
