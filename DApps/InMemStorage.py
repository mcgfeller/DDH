""" Example DApp - fake Coop Supercard data """
from __future__ import annotations


import typing

import fastapi
import fastapi.security
from utils.pydantic_utils import DDHbaseModel
from core import (common_ids, dapp_attrs, keys, nodes, principals, users,
                  schemas, transactions, errors, versions)

from schema_formats import py_schema
from backend import storage
from frontend import fastapi_dapp, fastapi_transactionable, sessions, user_auth
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)
app.include_router(fastapi_transactionable.router)


def get_apps() -> tuple[dapp_attrs.DApp]:
    return (IN_MEM_STORAGE_DAPP,)


fastapi_dapp.get_apps = get_apps


class _missing_class(DDHbaseModel): pass  # marker class


_missing = _missing_class()


class WriteAction(transactions.Action):
    key: common_ids.PersistId
    data: bytes | _missing_class

    def added(self, transaction: transactions.Transaction):
        """ Callback after transaction is added """
        transaction.trx_local[self.key] = self.data
        return

    async def commit(self, transaction):
        """ commit an action, called by transaction.commit() """

        if self.data is _missing:
            storage.Storage.delete(self.key, transaction)
        else:
            assert isinstance(self.data, bytes)
            storage.Storage.store(self.key, self.data, transaction)

    async def rollback(self, transaction):
        """ rollback an action, called by transaction.rollback() """
        transaction.trx_local.pop(self.key)
        return


@app.get("/storage/{key}")
async def load(
    key: common_ids.PersistId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
) -> bytes:
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    data = trx.trx_local.get(key, _missing)
    if data is _missing:
        try:
            data = storage.Storage.load(key, trx)
        except KeyError:
            trx.trx_local[key] = _missing  # trx acts as cache
            raise errors.NotFound(f'{key=} not found').to_http()
        trx.trx_local[key] = data  # found, cache it in trx
    else:
        assert isinstance(data, bytes)
    return data


@app.put("/storage/{key}")
async def store(
    key: common_ids.PersistId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
    data: bytes = fastapi.Body(..., media_type='data/binary')
):
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    trx.add(WriteAction(key=key, data=data))
    return


# Note: Post is not available, as we don't generate keys

@app.delete("/storage/{key}")
async def delete(
    key: common_ids.PersistId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
):
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    # print(f'storage.delete {key=}, {trx.trxid=}, ')
    trx.add(WriteAction(key=key, data=_missing))
    return


@app.delete("/storage")
async def purge_all(
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    confirm: bool = fastapi.Query(default=False, description="must be explicity True")
):
    """ Purge all storage, testing only 
        TODO: This should be testing user only. Introduce a tester privilege? 
    """
    if confirm:
        storage.Storage.byId.clear()
    return


class InMemStorageDApp(dapp_attrs.DApp):

    _ddhschema: py_schema.PySchemaElement = None
    version: versions.Version = '0.0'
    owner: typing.ClassVar[principals.Principal] = users.SystemUser
    catalog: common_ids.CatalogCategory = common_ids.CatalogCategory.system

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

    def get_schemas(self) -> dict[keys.DDHkey, schemas.AbstractSchema]:
        """ This DApp has not schema """
        return {}

    def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        raise ValueError(f'Unsupported {req.op=}')
        return d


IN_MEM_STORAGE_DAPP = InMemStorageDApp(name='InMemStorageDApp', owner=users.SystemUser,
                                       catalog=common_ids.CatalogCategory.system)


if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 9051
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
    ...
