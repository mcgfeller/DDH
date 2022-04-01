""" Set up a few fake Data Apps """
from __future__ import annotations
import datetime
import typing

import pydantic
from core import keys,permissions,schemas,nodes,keydirectory,principals,transactions,relationships
from core import dapp


class SampleDApps(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] =  principals.User(id='coop',name='Coop (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] 
    transforms_into : typing.ClassVar[typing.Optional[keys.DDHkey]]= None

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self.references = relationships.Reference.provides(self.schemakey)  + \
            relationships.Reference.provides(self.transforms_into)
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.Schema]:
        """ Obtain initial schema for DApp """
        return {self.schemakey:schemas.PySchema(schema_element=DummySchema)}


    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        if op == nodes.Ops.get:
            here,selection = access.ddhkey.split_at(key_split)
            d = {}
        else:
            raise ValueError(f'Unsupported {op=}')
        return d


    @classmethod
    def bootstrap(cls,session,pillars : dict) -> tuple[dapp.DApp]:
        apps = (

            cls(
                id='SwisscomEmpDApp',
                owner=principals.User(id='swisscom',name='Swisscom (fake account)'),
                schemakey = keys.DDHkey(key="//org/swisscom.com"), # TODO: would like to have subkey - but complains that parent doesn't exist
                transforms_into = keys.DDHkey(key="//p/employment/salary/statements"),
                ),
        )
        return  apps

class DummySchema(schemas.SchemaElement): ...

