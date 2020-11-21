""" Example DApp - fake Coop Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import core
from core import dapp


class CoopDApp(dapp.DApp):

    owner : typing.ClassVar[core.Principal] =  core.User(id='mgf',name='Martin')
    schemakey : typing.ClassVar[core.DDHkey] = core.DDHkey(key="/ddh/shopping/stores/coop")
 
    def get_schema(self) -> core.Schema:
        """ Obtain initial schema for DApp """
        return core.PySchema(schema_element=CoopSchema)



class CoopClient(core.SchemaElement):

    id = core.Principal
    supercard : typing.Optional[int] = None

    

class CoopSchema(core.SchemaElement):

    clients : list[CoopClient] = []


