""" Proxy representing DApps in runner  """
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
            return 'InProcessStorageResource'

    @classmethod
    def create(cls, id):
        """ create DAppResource from Id """
        try:
            return super().create(id)
        except errors.NotSelectable:
            return InProcessStorageResource(dapp=None)

    async def load(self, key: str, trx: transactions.Transaction) -> bytes:
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', verb='get', jwt=trx.user_token)
        return d

    async def store(self, key: str, data: bytes, trx: transactions.Transaction):
        print('data', data)
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', content=data, headers={'Content-Type': 'data/binary'}, verb='put', jwt=trx.user_token)
        return d

    async def delete(self, key: str, trx: transactions.Transaction):
        d = await self.dapp.send_url(f'/storage/{key}?trxid={trx.trxid}', verb='delete', jwt=trx.user_token)
        return d


class InProcessStorageResource(StorageResource):

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
