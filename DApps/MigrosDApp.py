""" Example DApp - fake Migros Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import core
from core import dapp

class MigrosDApp(dapp.DApp):

    owner : typing.ClassVar[core.Principal] =  core.User(id='mgf',name='Martin')
    schemakey : typing.ClassVar[core.DDHkey] = core.DDHkey(key="/ddh/shopping/stores/migros")
 
    def get_schema(self) -> core.Schema:
        """ Obtain initial schema for DApp """
        return core.PySchema(schema_element=MigrosSchema)

class Receipt(core.SchemaElement):

    Datum:      datetime.date 
    Zeit:       datetime.time 
    Filiale:    str
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0

class MigrosClient(core.SchemaElement):

    id = core.Principal
    cumulus : typing.Optional[int] = None
    receipts: list[Receipt] = []
    

class MigrosSchema(core.SchemaElement):

    clients : list[MigrosClient] = []


