""" Example DApp - fake Migros Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import keys,permissions,schemas
from core import dapp

class MigrosDApp(dapp.DApp):

    owner : typing.ClassVar[permissions.Principal] =  permissions.User(id='migros',name='Migros (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="/ddh/shopping/stores/migros")
    _ddhschema : schemas.SchemaElement = None


    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = MigrosSchema()
 
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp """
        return schemas.PySchema(schema_element=MigrosSchema)

    def execute(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        here,selection = access.ddhkey.split_at(key_split)
        d = self._ddhschema.get_data(selection,access,q)
        return d

class Receipt(schemas.SchemaElement):

    Datum:      datetime.date 
    Zeit:       datetime.time 
    Filiale:    str
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0

    @classmethod
    def resolve(cls,remainder, ids, q):
        principals = ids.get(MigrosClient,{}).get('id', [])

        return {}

class MigrosClient(schemas.SchemaElement):

    id : permissions.Principal
    cumulus : typing.Optional[int] = None
    receipts: list[Receipt] = []
    


    

class MigrosSchema(schemas.SchemaElement):

    clients : list[MigrosClient] = []


    def get_data(self, selection: keys.DDHkey,access: permissions.Access, q):
        d = self.get_resolver(selection,access,q)
        return {}






