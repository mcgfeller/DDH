""" Example DApp - fake Migros Cumulus data _ V2 MICRO SERVICE """
from __future__ import annotations

import fastapi
import fastapi.security
import typing
import pydantic
import datetime
import enum
import httpx
import os



from core import dapp_attrs
from core import keys,permissions,schemas,facade,errors,principals
from frontend import sessions

import datetime
import typing

import pandas  # for example
import pydantic
import httpx
from glom import Iter, S, T, glom  # transform

app = fastapi.FastAPI(version="0.0")

from frontend import user_auth # provisional user management




from core import ( keys, nodes, permissions, principals,
                  relationships, schemas, transactions, common_ids, versions,dapp_attrs)


CLIENT = httpx.AsyncClient(timeout=5,base_url='http://localhost:8001') # TODO: Configure or determine URL

@app.on_event("startup")
async def startup_event():
    """ Connect ourselves """
    location = f"http://localhost:{os.environ.get('port')}" # our own port is in the environment
    d = dapp_attrs.RunningDApp(id=MIGROS_DAPP.id,dapp_version=versions.Version(app.version),schema_version=versions.Version('0.0'),location=location)
    await CLIENT.post('connect',data=d.json())



@app.on_event("shutdown")
async def shutdown_event():   
    return

@app.get("/app_info")
async def get_app_info():
    d = {MIGROS_DAPP.id: MIGROS_DAPP.dict()}
    return d

@app.get("/schemas")
async def get_schemas() -> dict:
    s = {str(k): (s.schema_attributes,s.to_output()) for k,s in MIGROS_DAPP.get_schemas().items()}
    return s

@app.post("/execute")
async def execute(req : dapp_attrs.ExecuteRequest):
    return MIGROS_DAPP.execute(req)


@app.get("/provide/ddh{docpath:path}")
async def get_data(
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    modes: set[permissions.AccessMode] = {permissions.AccessMode.read},
    q: str = fastapi.Query(None, alias="item-query"),
    ):

    access = permissions.Access(op = permissions.Operation.get, ddhkey = keys.DDHkey(docpath),principal=session.user, modes = modes, byDApp=session.dappid)
    try:
        d = facade.ddh_get(access,session,q)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"ddhkey": access.ddhkey, "res": d}


class MigrosDApp(dapp_attrs.DApp):

    _ddhschema : schemas.SchemaElement = None

    



    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._ddhschema = MigrosSchema()
        self.transforms_into = keys.DDHkey(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey) + \
            relationships.Reference.provides(self.transforms_into)
        # self.register_transform(transforms_into)
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {keys.DDHkey(key="//org/migros.ch"):schemas.PySchema(schema_element=MigrosSchema)}


    def execute(self, req : dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        if req.op == nodes.Ops.get:
            here,selection = req.access.ddhkey.split_at(req.key_split)
            d = self._ddhschema.get_data(selection,req.access,req.q)
        else:
            raise ValueError(f'Unsupported {req.op=}')
        return d




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


class MigrosSchema(schemas.SchemaElement):

    cumulus : typing.Optional[int] = pydantic.Field(None,sensitivity=schemas.Sensitivity.qi)
    receipts: list[Receipt] = []


    def get_data(self, selection: keys.DDHkey,access: permissions.Access, q):
        d = self.get_resolver(selection,access,q)
        return d

MIGROS_DAPP = MigrosDApp(owner=principals.User(id='migros',name='Migros (fake account)'),
    schemakey=keys.DDHkey(key="//org/migros.ch"),
    catalog = common_ids.CatalogCategory.living)