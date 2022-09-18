""" Example DApp - fake Coop Supercard data """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, permissions, principals,
                  relationships, schemas)

from frontend import fastapi_dapp
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)



def get_apps() -> tuple[dapp_attrs.DApp]:
    return (COOP_DAPP,)

fastapi_dapp.get_apps = get_apps

class CoopDApp(dapp_attrs.DApp):

    _ddhschema : schemas.SchemaElement = None
    version = '0.2'

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = CoopSchema()
        self.transforms_into = keys.DDHkey(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey) + \
            relationships.Reference.provides(self.transforms_into)
        # self.register_transform(transforms_into)
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {keys.DDHkey(key="//org/coop.ch"):schemas.PySchema(schema_element=CoopSchema)}


    def execute(self, req : dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        if req.op == nodes.Ops.get:
            here,selection = req.access.ddhkey.split_at(req.key_split)
            d = self._ddhschema.get_data(selection,req.access,req.q)
        else:
            raise ValueError(f'Unsupported {req.op=}')
        return d

    owner : typing.ClassVar[principals.Principal] =  principals.User(id='coop',name='Coop (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="//org/coop.ch")
    catalog = common_ids.CatalogCategory.living


    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = CoopSchema()
        transforms_into = keys.DDHkey(key="//p/living/shopping/receipts")
        self.references =  relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey)  + \
            relationships.Reference.provides(transforms_into)
        # self.register_transform(transforms_into)
 




class CoopSchema(schemas.SchemaElement):

    supercard : typing.Optional[int] = pydantic.Field(None,sensitivity=schemas.Sensitivity.qi)
    #receipts: list[Receipt] = []

COOP_DAPP = CoopDApp(owner=principals.User(id='coop',name='Coop (fake account)'),
    schemakey=keys.DDHkey(key="//org/coop.ch"),
    catalog = common_ids.CatalogCategory.living)
