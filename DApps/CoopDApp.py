""" Example DApp - fake Coop Supercard data """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, permissions, principals, users,
                  relationships, schemas, versions)

from schema_formats import py_schema
from frontend import fastapi_dapp
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)


def get_apps() -> tuple[dapp_attrs.DApp]:
    return (COOP_DAPP,)


fastapi_dapp.get_apps = get_apps


class CoopDApp(dapp_attrs.DApp):

    _ddhschema: py_schema.PySchemaElement = None
    version: versions.Version = '0.2'
    owner: typing.ClassVar[principals.Principal] = users.User(
        id='coop', name='Coop (fake account)')
    schemakey: typing.ClassVar[keys.DDHkeyVersioned] = keys.DDHkeyVersioned0(key="//org/coop.ch")
    catalog: common_ids.CatalogCategory = common_ids.CatalogCategory.living

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._ddhschema = CoopSchema()

        self.transforms_into = keys.DDHkeyVersioned0(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey) \
            # TODO: Must not register transform unless provided:
        # + relationships.Reference.provides(self.transforms_into)

    def get_schemas(self) -> dict[keys.DDHkey, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {keys.DDHkeyVersioned0(key="//org/coop.ch"): py_schema.PySchema(schema_element=CoopSchema)}

    def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        if req.op == nodes.Ops.get:
            here, selection = req.access.ddhkey.split_at(req.key_split)
            # key we transform into?
            if req.access.ddhkey.without_owner() == self.transforms_into:
                d = self.get_and_transform(req)
            else:  # key we provide, call schema descent to resolve:
                d = self._ddhschema.get_data(selection, req.access, req.q)
        else:
            raise ValueError(f'Unsupported {req.op=}')
        return d

    def get_and_transform(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by transforming key, then executing, then transforming result """
        return


class CoopSchema(py_schema.PySchemaElement):
    """ There is no Schema for Coop yet """

    supercard: int | None = py_schema.SchemaField(None, sensitivity=schemas.Sensitivity.qid)
    receipts: list[py_schema.PySchemaReference.create_from_key(
        keys.DDHkeyRange('//p/living/shopping/receipts:::>=0'))] = []

    # def get_data(self, selection: keys.DDHkey, access: permissions.Access, q):
    #     return None


COOP_DAPP = CoopDApp(owner=users.User(id='coop', name='Coop (fake account)'),
                     schemakey=keys.DDHkeyVersioned0(key="//org/coop.ch"),
                     catalog=common_ids.CatalogCategory.living)

if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 9022
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
    ...
