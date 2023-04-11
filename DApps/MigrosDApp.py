""" Example DApp - fake Migros Cumulus data _ V2 MICRO SERVICE """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pandas  # for example
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, permissions, users,
                  relationships, schemas, errors, versions, capabilities)
from schema_formats import py_schema
from utils import key_utils
from glom import Iter, S, T, glom  # transform

from frontend import fastapi_dapp, user_auth
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)


def get_apps() -> tuple[dapp_attrs.DApp]:
    return (MIGROS_DAPP,)


fastapi_dapp.get_apps = get_apps


class MigrosDApp(dapp_attrs.DApp):

    version = '0.2'

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.transforms_into = keys.DDHkeyVersioned0(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey) + \
            relationships.Reference.provides(self.transforms_into)

    def get_schemas(self) -> dict[keys.DDHkey, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        caps = {capabilities.Capabilities.Anonymize, capabilities.Capabilities.Pseudonymize}
        sa = schemas.SchemaAttributes(version=versions.Version(self.version), capabilities=caps)
        return {keys.DDHkeyVersioned0(key="//org/migros.ch"): py_schema.PySchema(schema_element=MigrosSchema,
                                                                                 schema_attributes=sa)}

    def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        if req.op == nodes.Ops.get:
            here, selection = req.access.ddhkey.split_at(req.key_split)
            # key we transform into?
            if req.access.ddhkey.without_owner().without_variant_version() == self.transforms_into.without_variant_version():
                d = self.get_and_transform(req)
            else:  # key we provide, call schema descent to resolve:
                d = self.get_data(selection, req.access, req.q)
        else:
            raise ValueError(f'Unsupported {req.op=}')
        return d

    def get_data(self, selection: keys.DDHkey, access: permissions.Access, q):
        # print(f'MigrosSchema.get_data: {selection=}')
        root, resolve = Resolvers.most_specific(selection)
        if not resolve:
            raise errors.NotFound(f'Incomplete key: {selection}').to_http()
        remainder = selection.remainder(len(root.key))
        princs = user_auth.get_principals(access.ddhkey.owners)
        res = resolve(remainder, princs, q)
        return res

    def get_and_transform(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by transforming key, then executing, then transforming result """
        here, selection = req.access.ddhkey.split_at(req.key_split)
        selection2 = keys.DDHkey(('receipts',))  # insert selection
        d = self.get_data(selection2, req.access, req.q)  # obtain org-format data
        # transform with glom: into list of dicts, whereas item key becomes buyer:
        spec = {
            "items": (
                T.items(),
                Iter((S(value=T[0]), T[1], [{'buyer': S.value, 'article': 'Artikel', 'quantity': 'Menge',
                     'amount': 'Umsatz', 'when': 'Datum_Zeit', 'where': 'Filiale'}])).flatten(),
                list,
            )
        }
        s = glom(d, spec)
        return s


class ProduktDetail(py_schema.PySchemaElement):
    """ Details of a product """
    produkt_kategorie: str
    garantie: str | None = None
    garantie_jahre: int | None = 1
    beschreibung: str = ''
    labels: list[str] = []


class Receipt(py_schema.PySchemaElement):

    """ The Receipt of an individual purchase """

    Datum_Zeit: datetime.datetime = pydantic.Field(sensitivity=schemas.Sensitivity.sa)
    Filiale:    str = pydantic.Field(sensitivity=schemas.Sensitivity.sa)
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0
    Produkt: ProduktDetail | None = None

    @classmethod
    def resolve(cls, remainder, principals, q):
        data = {}
        for principal in principals:
            d = cls.get_cumulus_json(principal, q)
            data[principal.id] = d
        return data

    @classmethod
    def get_cumulus_json(cls, principal, q):
        """ This is extremly fake to retrieve data for my principal """
        if principal.id == 'mgf':
            df = pandas.read_csv(r"C:\Projects\DDH\DApps\test_data_migros.csv",
                                 parse_dates=[['Datum', 'Zeit']], dayfirst=True)
            d = df.to_dict(orient='records')
        else:
            d = {}
        return d


class MigrosSchema(py_schema.PySchemaElement):
    """ A fake Migros schema, showing Cumulus receipts """
    cumulus: int | None = pydantic.Field(None, sensitivity=schemas.Sensitivity.qid)
    receipts: list[Receipt] = []


Resolvers = key_utils.LookupByKey({  # register the resolvers per schema key
    keys.DDHkey('receipts'): Receipt.resolve,
})


MIGROS_DAPP = MigrosDApp(owner=users.User(id='migros', name='Migros (fake account)'),
                         schemakey=keys.DDHkeyVersioned0(key="//org/migros.ch"),
                         catalog=common_ids.CatalogCategory.living)


if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 9002
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
