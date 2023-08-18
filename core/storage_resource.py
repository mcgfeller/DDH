""" Proxy representing DApps in runner  """
from __future__ import annotations

import pydantic.json
import logging

logger = logging.getLogger(__name__)

# from frontend import sessions
from core import dapp_proxy, transactions


class StorageResource(dapp_proxy.DAppResource):

    async def added(self, trx: transactions.Transaction):
        """ Issue begin transaction req to DApp """
        print(f'*DAppResource added {trx=}, {trx.user_token=}')
        await self.begin(trx)
        return

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
