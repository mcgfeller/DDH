""" Example DApp - fake Coop Cumulus data """
from __future__ import annotations
import datetime
import typing

import pydantic
from core import keys,permissions,schemas,nodes,keydirectory,principals,transactions,relationships
from core import dapp


class CoopDApp(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] =  principals.User(id='coop',name='Coop (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="//org/coop.ch")

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = CoopSchema()
        transforms_into = keys.DDHkey(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.provides(self.schemakey)  + \
            relationships.Reference.provides(transforms_into)
        # self.register_transform(transforms_into)
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.Schema]:
        """ Obtain initial schema for DApp """
        return {keys.DDHkey(key="//org/coop.ch"):schemas.PySchema(schema_element=CoopSchema)}


    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        if op == nodes.Ops.get:
            here,selection = access.ddhkey.split_at(key_split)
            d = {}
        else:
            raise ValueError(f'Unsupported {op=}')
        return d






class CoopSchema(schemas.SchemaElement):

    supercard : typing.Optional[int] = pydantic.Field(None,sensitivity=schemas.Sensitivity.qi)
    #receipts: list[Receipt] = []


