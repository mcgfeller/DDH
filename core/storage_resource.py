""" StorageResoure with DApp as an executor.
    Also provides in-memory storage resource for testing. 
"""
from __future__ import annotations

import pydantic.json
import logging

logger = logging.getLogger(__name__)

# from frontend import sessions
from core import dapp_proxy, transactions, common_ids, errors
from backend import storage


class StorageResource(dapp_proxy.DAppResource):

    @property
    def id(self) -> str:
        """ key of resource, to be stored in transaction """
        if self.dapp:
            assert self.dapp.attrs.id
            return self.dapp.attrs.id
        else:
            return '?'  # 'InProcessStorageResource'

    async def load(self, key: str, trx: transactions.Transaction) -> bytes:
        assert self.dapp
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', verb='get', jwt=trx.user_token)
        return d

    async def store(self, key: str, data: bytes, trx: transactions.Transaction):
        assert self.dapp
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', content=data, headers={'Content-Type': 'data/binary'}, verb='put', jwt=trx.user_token)
        return d

    async def delete(self, key: str, trx: transactions.Transaction):
        assert self.dapp
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', verb='delete', jwt=trx.user_token)
        return d


class InProcessStorageResource(StorageResource):
    """ Storage ressource that runs without a DApp, for testing only.
        Uses storage.Storage to store. 
        Transaction support is empty. 
    """

    async def begin(self, trx: transactions.Transaction):
        return

    async def commit(self, trx: transactions.Transaction):
        return

    async def abort(self, trx: transactions.Transaction):
        return

    async def load(self, key: common_ids.PersistId, trx: transactions.Transaction) -> bytes:
        return storage.Storage.load(key, trx)

    async def store(self, key: common_ids.PersistId, data: bytes, trx: transactions.Transaction):
        return storage.Storage.store(key, data, trx)

    async def delete(self, key: common_ids.PersistId, trx: transactions.Transaction):
        return storage.Storage.delete(key, trx)
