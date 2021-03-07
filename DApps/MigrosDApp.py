""" Example DApp - fake Migros Cumulus data """
from __future__ import annotations
import datetime
import typing

from core import keys,permissions,schemas,nodes
from core import dapp

import pandas # for example
from glom import glom,S,T,Iter # transform

class MigrosDApp(dapp.DApp):

    owner : typing.ClassVar[permissions.Principal] =  permissions.User(id='migros',name='Migros (fake account)')
    schemakey : typing.ClassVar[keys.DDHkey] = keys.DDHkey(key="/org/living/stores/migros.ch")
    _ddhschema : schemas.SchemaElement = None


    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = MigrosSchema()
        self.register_transform()
 
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp """
        return schemas.PySchema(schema_element=MigrosSchema)

    def execute(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        here,selection = access.ddhkey.split_at(key_split)
        d = self._ddhschema.get_data(selection,access,q)
        return d


    def register_transform(self):
        ddhkey = keys.DDHkey('/p/living/shopping/receipts')
        de_node = nodes.NodeRegistry[ddhkey].get(nodes.NodeType.execute)
        if not de_node:
            de_node = nodes.DelegatedExecutableNode(owner=self.owner)
            de_node.executors.append(self)
            nodes.NodeRegistry[ddhkey] = de_node
        return

    def get_and_transform(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        """ obtain data by transforming key, then executing, then transforming result """
        here,selection = access.ddhkey.split_at(key_split)
        selection2 = keys.DDHkey(('clients',selection.key[0],'receipts')) # insert selection
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

    Datum_Zeit: datetime.datetime
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
        data = {}
        for principal in principals:
            d = cls.get_cumulus_json(principal,q)
            data[principal.id] = d
        return data

    @classmethod
    def get_cumulus_json(cls,principal,q):
        """ This is extrmly fake to retrieve data for my principal """
        if principal.id =='mgf':
            df = pandas.read_csv(r"C:\Projects\DDH\DApps\test_data_migros.csv",parse_dates = [['Datum','Zeit']],dayfirst=True)
            d = df.to_dict(orient='records')
        else:
            d = {}
        return d


class MigrosClient(schemas.SchemaElement):

    id : permissions.Principal
    cumulus : typing.Optional[int] = None
    receipts: list[Receipt] = []
    


    

class MigrosSchema(schemas.SchemaElement):

    clients : list[MigrosClient] = []


    def get_data(self, selection: keys.DDHkey,access: permissions.Access, q):
        d = self.get_resolver(selection,access,q)
        return d





