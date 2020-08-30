""" Example DApp - fake Migros Cumulus data """
from __future__ import annotations
import datetime
import core
import dapp
import typing

class MigrosDApp(dapp.DApp):

    owner : typing.ClassVar[core.Principal] =  core.User(id='mgf',name='Martin')
    schemakey : typing.ClassVar[core.DDHkey] = core.DDHkey(key="/ddh/shopping/stores/migros")
 
    def get_schema(self) -> core.Schema:
        jschema = core.JsonSchema(json_schema=MigrosSchema.schema_json())
        return jschema

class Receipt(core.NoCopyBaseModel):

    Datum:      datetime.date 
    Zeit:       datetime.time 
    Filiale:    str
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0

class MigrosClient(core.NoCopyBaseModel):

    id = core.Principal
    cumulus : typing.Optional[int] = None
    receipts: typing.List[Receipt] = []
    

class MigrosSchema(core.NoCopyBaseModel):

    clients : typing.List[MigrosClient] = []


