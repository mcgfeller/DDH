""" Example DApp - fake Coop Supercard data """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, principals, users,
                  schemas, transactions, errors)

from schema_formats import py_schema
from backend import storage
from frontend import fastapi_dapp, sessions, user_auth
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)


def get_apps() -> tuple[dapp_attrs.DApp]:
    return (IN_MEM_STORAGE_DAPP,)


fastapi_dapp.get_apps = get_apps

_missing = object()


@app.post("/transaction/{trxid}/begin")
async def trx_begin(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    print(f"/transaction/{trxid}/begin")
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    return trx.trxid


@app.post("/transaction/{trxid}/commit")
async def trx_commit(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    trx = transactions.Transaction.Transactions.get(trxid)
    if trx:
        for key, data in trx.trx_local.items():
            if data is _missing:
                storage.Storage.delete(typing.cast(common_ids.PersistId,  key), trx)
            else:
                storage.Storage.store(typing.cast(common_ids.PersistId,  key), data, trx)
        trx.commit()
    else:
        raise errors.NotFound('Transaction not found').to_http()

    return trx.trxid


@app.post("/transaction/{trxid}/abort")
async def trx_abort(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    trx = transactions.Transaction.Transactions.get(trxid)
    if trx:
        trx.trx_local.clear()
        trx.abort()
    else:
        raise errors.NotFound('Transaction not found').to_http()
    return trx.trxid


@app.get("/storage/{key}")
async def load(
    key: str,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
) -> bytes:
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    print(f'storage.load {key=}, {trx.trxid=}')
    data = trx.trx_local.get(key, _missing)
    if data is _missing:
        try:
            data = storage.Storage.load(typing.cast(common_ids.PersistId,  key), trx)
        except KeyError:
            trx.trx_local[key] = _missing  # trx acts as cache
            raise errors.NotFound(f'{key=} not found').to_http()
        trx.trx_local[key] = data
    else:
        assert isinstance(data, bytes)
    return data


@app.put("/storage/{key}")
async def store(
    key: str,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
    data: bytes = fastapi.Body(media_type='data/binary')
):
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    print(f'storage.store {key=}, {trx.trxid=}, {data=}')
    trx.trx_local[key] = data
    # storage.Storage.store(typing.cast(common_ids.PersistId,  key), data, trx)
    return


# Note: Post is not available, as we don't generate keys

@app.delete("/storage/{key}")
async def delete(
    key: str,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    trxid: common_ids.TrxId = fastapi.Query(),
):
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    print(f'storage.delete {key=}, {trx.trxid=}, ')
    trx.trx_local[key] = _missing
    return


@app.delete("/storage")
async def purge_all(
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    confirm: bool = fastapi.Query(default=False, description="must be explicity True")
):
    if confirm:
        storage.Storage.byId.clear()
    return


class InMemStorageDApp(dapp_attrs.DApp):

    _ddhschema: py_schema.PySchemaElement = None
    version = '0.0'
    owner: typing.ClassVar[principals.Principal] = users.SystemUser
    catalog = common_ids.CatalogCategory.system

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

    def get_schemas(self) -> dict[keys.DDHkey, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {}

    def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        if req.op == nodes.Ops.get:
            here, selection = req.access.ddhkey.split_at(req.key_split)
        else:
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
