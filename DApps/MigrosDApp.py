""" Example DApp - fake Migros Cumulus data """
from __future__ import annotations
import datetime
import typing

import pydantic

from core import keys,permissions,schemas,nodes,keydirectory,principals,transactions
from core import dapp

import pandas # for example
from glom import glom,S,T,Iter # transform

class MigrosDApp(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] =  principals.User(id='migros',name='Migros (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="//org/migros.ch")
    _ddhschema : schemas.SchemaElement = None


    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = MigrosSchema()
        self.register_transform()
 
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp """
        return schemas.PySchema(schema_element=MigrosSchema)

    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        if op == nodes.Ops.get:
            here,selection = access.ddhkey.split_at(key_split)
            d = self._ddhschema.get_data(selection,access,q)
        else:
            raise ValueError(f'Unsupported {op=}')
        return d


    def register_transform(self):
        ddhkey = keys.DDHkey('//p/living/shopping/receipts')
        de_node = keydirectory.NodeRegistry[ddhkey].get(nodes.NodeSupports.execute)
        if not de_node:
            de_node = nodes.DelegatedExecutableNode(owner=self.owner)
            de_node.executors.append(self)
            keydirectory.NodeRegistry[ddhkey] = de_node
        return

    def get_and_transform(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        """ obtain data by transforming key, then executing, then transforming result """
        here,selection = access.ddhkey.split_at(key_split)
        selection2 = keys.DDHkey(('receipts',)) # insert selection
        d = self._ddhschema.get_data(selection2,access,q) # obtain org-format data
        # transform with glom: into list of dicts, whereas item key becomes buyer: 
        spec = {
             "items": (
              T.items(),
              Iter((S(value=T[0]),T[1],[{'buyer':S.value,'article':'Artikel','quantity':'Menge','amount':'Umsatz','when': 'Datum_Zeit','where':'Filiale'}])).flatten(),
              list,
             )
         }
        s = glom(d,spec)
        return s
    


class Receipt(schemas.SchemaElement):

    Datum_Zeit: datetime.datetime = pydantic.Field(sensitivity= schemas.Sensitivity.sa)
    Filiale:    str = pydantic.Field(sensitivity= schemas.Sensitivity.sa)
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0

    @classmethod
    def resolve(cls,remainder, principals, q):
        data = {}
        for principal in principals:
            d = cls.get_cumulus_json(principal,q)
            data[principal.id] = d
        return data

    @classmethod
    def get_cumulus_json(cls,principal,q):
        """ This is extremly fake to retrieve data for my principal """
        if principal.id =='mgf':
            df = pandas.read_csv(r"C:\Projects\DDH\DApps\test_data_migros.csv",parse_dates = [['Datum','Zeit']],dayfirst=True)
            d = df.to_dict(orient='records')
        else:
            d = {}
        return d


# class MigrosClient(schemas.SchemaElement):

#     # id : principals.Principal = pydantic.Field(sensitivity= schemas.Sensitivity.ei)

    


    

class MigrosSchema(schemas.SchemaElement):

    cumulus : typing.Optional[int] = pydantic.Field(None,sensitivity=schemas.Sensitivity.qi)
    receipts: list[Receipt] = []


    def get_data(self, selection: keys.DDHkey,access: permissions.Access, q):
        d = self.get_resolver(selection,access,q)
        return d





