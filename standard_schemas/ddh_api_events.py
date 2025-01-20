""" Schemas //org/ddh/events, providing an API against principaled information about DDH itself: Events
"""
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs, nodes
from utils import utils, queues
from frontend import sessions
from schema_formats import py_schema


class Subscriptions(py_schema.PySchemaElement):
    # TODO
    """ Grants (with Schema) """
    subscribe: dict[str, permissions.Consents] = {}


class EventQuery(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        """ obtain given and received consents and return them as a Grants object, which combines
            the key and its consents. 
        """

        op = req.access.ddhkey.split_at(req.key_split)[1]
        principal = req.access.principal
        assert principal
        print(f'{req.access=}')

        match str(op).lower():
            case 'subscribe':  # TOOD:#35
                return 'subscribe'

            case 'wait':  # TOOD:#35
                return 'wait'

            case _:
                raise errors.NotFound(f"Selection {op} not found; must be 'subscribe' or 'given'")
        r = {}
        return r

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.key: Subscriptions.to_schema()}  # TOOD:#35


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    attrs = dapp_attrs.SchemaProvider(references=[])
    cq = EventQuery(owner=principals.RootPrincipal, attrs=attrs, key=keys.DDHkeyVersioned0('//org/ddh/events'))
    cq.register(session)


install()
