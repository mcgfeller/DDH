""" Example DApp - fake Migros Cumulus data """

import datetime
from .. import core
from .. import dapp

class MigrosDApp(dapp.DApp):
    
 
    def get_schema(self) -> core.Schema:
        jschema = core.JsonSchema(json_schema=MigrosSchema.schema_json())
        return jschema

    

class MigrosSchema(core.NoCopyBaseModel):


    Datum:      datetime.date 
    Zeit:       datetime.time 
    Filiale:    str
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0

