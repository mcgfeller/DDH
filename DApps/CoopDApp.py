""" Example DApp - fake Coop Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import keys,schemas,principals
from core import dapp


class CoopDApp(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] =  principals.User(id='mgf',name='Martin')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="//org/coop.ch")
 
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp """
        return schemas.PySchema(schema_element=CoopSchema)



class CoopClient(schemas.SchemaElement):

    id = principals.Principal
    supercard : typing.Optional[int] = None

    

class CoopSchema(schemas.SchemaElement):

    clients : list[CoopClient] = []


