""" Schemas /org/ddh, providing an API against principaled information about DDH itself """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs
from frontend import sessions
from schema_formats import py_schema


class Consent(py_schema.PySchemaElement):
    principal: common_ids.PrincipalId


class Consents(py_schema.PySchemaElement):
    receipts: list[Consent]


class ConsentQuery(nodes.ExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        return {}


def install():
    transaction = sessions.get_system_session().get_or_create_transaction()
    return Consents.insert_as_schema(transaction, keys.DDHkeyGeneric('//org/ddh/consents'))


install()
