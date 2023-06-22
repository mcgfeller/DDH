""" Example DApp - fake Coop Supercard data """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, permissions, principals, users,
                  relationships, schemas)

from schema_formats import py_schema
from frontend import fastapi_dapp
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)


def get_apps() -> tuple[dapp_attrs.DApp]:
    return (IN_MEM_STORAGE_DAPP,)


fastapi_dapp.get_apps = get_apps


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
