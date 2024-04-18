""" Schemas /org/ddh, providing an API against principaled information about DDH itself """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs
from frontend import sessions
from schema_formats import py_schema


class Consent(py_schema.PySchemaElement):
    principal: str  # common_ids.PrincipalId


class Consents(py_schema.PySchemaElement):
    receipts: list[Consent]


class ConsentQuery(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        # TODO: #34
        op = req.access.ddhkey.split_at(req.key_split)[1]
        match str(op).lower():
            case 'received':
                r = 'received'
            case 'given':
                r = 'given'
            case _:
                raise errors.NotFound(f"Selection {op} not found; must be 'received' or 'given'")

        return r

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.key: Consents.to_schema()}


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    # Consents.insert_as_schema(transaction, keys.DDHkeyGeneric('//org/ddh/consents'))
    attrs = dapp_attrs.SchemaProvider(references=[])
    cq = ConsentQuery(owner=principals.RootPrincipal, attrs=attrs, key=keys.DDHkeyVersioned0('//org/ddh/consents'))
    cq.register(session)


install()
