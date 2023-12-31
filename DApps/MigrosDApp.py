""" Example DApp - fake Migros Cumulus data _ V2 MICRO SERVICE """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pandas  # for example
import pydantic

from core import (common_ids, dapp_attrs, keys, nodes, permissions, users,
                  relationships, schemas, errors, versions, trait, principals)
from traits import anonymization
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

    version: versions.Version = '0.2'

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.transforms_into = keys.DDHkeyVersioned0(key="//p/living/shopping/receipts")
        self.references = relationships.Reference.defines(self.schemakey) + relationships.Reference.provides(self.schemakey) + \
            relationships.Reference.provides(self.transforms_into)

    def get_schemas(self) -> dict[keys.DDHkey, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp 
            We supply a schema at a current version and and old version 0.1 for testing purposes. 
        """
        caps = trait.Transformers(anonymization.Anonymize(), anonymization.Pseudonymize(),
                                  anonymization.DePseudonymize())
        sa = schemas.SchemaAttributes(version=versions.Version(self.version), transformers=caps)
        sa_prev = schemas.SchemaAttributes(version=versions.Version('0.1'), transformers=caps)
        return {keys.DDHkeyVersioned0(key="//org/migros.ch"): py_schema.PySchema(schema_element=MigrosSchema,
                                                                                 schema_attributes=sa),
                keys.DDHkeyVersioned(key="//org/migros.ch:::0.1"): py_schema.PySchema(schema_element=MigrosSchema,
                                                                                      schema_attributes=sa_prev),
                }  # type:ignore

    def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by recursing to schema """
        match req.op:
            case nodes.Ops.get:
                here, selection = req.access.ddhkey.split_at(req.key_split)
                # key we transform into?
                if req.access.ddhkey.without_owner().without_variant_version() == self.transforms_into.without_variant_version():
                    d = self.get_and_transform(req)
                else:  # key we provide, call schema descent to resolve:
                    d = self.get_data(selection, req.access, req.q)
            case nodes.Ops.put:
                d = req.data  # don't do anything at the moment
            case _:
                raise ValueError(f'Unsupported {req.op=}')
        return d

    def get_data(self, selection: keys.DDHkey, access: permissions.Access, q):
        top = MigrosSchema.descend_path(selection)
        if not top:
            raise errors.NotFound(f'Key not found: {selection}').to_http()
        remainder = selection.remainder(len(selection.key))
        print(f'MigrosSchema.get_data: {selection=}, {remainder=}')
        principals = user_auth.get_principals(access.ddhkey.owner)
        res = {}
        for principal in principals:  # resolve per principal
            res[principal.id] = top.resolve(remainder, principal, q)
        return res

    def get_and_transform(self, req: dapp_attrs.ExecuteRequest):
        """ obtain data by transforming key, then executing, then transforming result """
        here, selection = req.access.ddhkey.split_at(req.key_split)
        selection2 = keys.DDHkey(('receipts',))  # insert selection
        d = self.get_data(selection2, req.access, req.q)  # obtain org-format data
        principal = list(d.keys())[0]
        # transform with glom: into list of dicts, whereas item key becomes buyer:
        spec = {
            principal:
                {'receipts':
                 (
                     T.items(),
                     Iter((S(value=T[0]), T[1], [{'buyer': S.value, 'article': 'Artikel', 'quantity': 'Menge',
                                                  'amount': 'Umsatz', 'when': 'Datum_Zeit', 'where': 'Filiale'}])).flatten(),
                     list,
                 )
                 }
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

    Datum_Zeit: datetime.datetime = py_schema.SchemaField(sensitivity=schemas.Sensitivity.sa)
    Filiale:    str = py_schema.SchemaField(sensitivity=schemas.Sensitivity.sa)
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     float = 0
    Umsatz:     float = 0
    Produkt: ProduktDetail | None = None

    @classmethod
    def resolve(cls, remainder, principal, q) -> dict:
        data = cls.get_cumulus_json(principal, q)
        return data

    @classmethod
    def get_cumulus_json(cls, principal, q):
        """ This is extremly fake to retrieve data for my principal """
        if principal.id == 'mgf':
            df = pandas.read_csv(r"C:\Projects\DDH\DApps\test_data_migros.csv",
                                 parse_dates=[['Datum', 'Zeit']], date_format="%d-%m-%y %H:%M:%S")
            d = df.to_dict(orient='records')
        else:
            d = {}
        return d


class MigrosSchema(py_schema.PySchemaElement):
    """ A fake Migros schema, showing Cumulus receipts """
    cumulus: int | None = py_schema.SchemaField(None, sensitivity=schemas.Sensitivity.qid)
    receipts: list[Receipt] = []

    @classmethod
    def resolve(cls, remainder, principal, q) -> dict:
        # print(f'{cls}.resolve({remainder=}, {principal=}, {q=})')
        if principal.id == 'mgf':  # we only have data for this guy here
            d = super().resolve(remainder, principal, q)  # descend on all objects
            d['cumulus'] = 423
        else:
            d = {}
        return d


MIGROS_DAPP = MigrosDApp(owner=users.User(id='migros', name='Migros (fake account)'),
                         schemakey=keys.DDHkeyVersioned0(key="//org/migros.ch"),
                         catalog=common_ids.CatalogCategory.living)


if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 9002
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
