""" Example DApp - fake Coop Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import keys,permissions,schemas
from core import dapp


class CoopDApp(dapp.DApp):

    owner : typing.ClassVar[permissions.Principal] =  permissions.User(id='mgf',name='Martin')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="/ddh/shopping/stores/coop")
 
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp """
        return schemas.PySchema(schema_element=CoopSchema)



class CoopClient(schemas.SchemaElement):

    id = permissions.Principal
    supercard : typing.Optional[int] = None

    

class CoopSchema(schemas.SchemaElement):

    clients : list[CoopClient] = []


