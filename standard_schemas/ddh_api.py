""" Schemas /org/ddh, providing an API against principaled information about DDH itself """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs, consentcache, nodes
from utils import utils
from frontend import sessions
from schema_formats import py_schema


class Consent(py_schema.PySchemaElement):
    principal: str  # common_ids.PrincipalId


class Consents(py_schema.PySchemaElement):
    consents: list[Consent]


class ConsentQuery(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        # TODO: #34
        # consents_by_principal: dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, set[permissions.AccessMode]]]
        op = req.access.ddhkey.split_at(req.key_split)[1]
        match str(op).lower():
            case 'received':
                c = consentcache.ConsentCache.consents_by_principal.get(req.access.principal.id, {})
                r = c
            case 'given':
                # we get all keys descending from the owner key:
                owner_key = keys.DDHkey(req.access.principal.id).ensure_rooted()
                node_keys = keydirectory.NodeRegistry.get_keys_with_prefix(owner_key)
                # get the consent nodes, and consents
                r = {}
                for cnode in await keydirectory.NodeRegistry.get_nodes_from_tuple_keys(node_keys, nodes.NodeSupports.consents, req.transaction):
                    # consent node by key:
                    r[str(cnode.key)] = cnode.consents
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
