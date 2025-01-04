""" Schemas /org/ddh, providing an API against principaled information about DDH itself """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs, consentcache, nodes
from utils import utils
from frontend import sessions
from schema_formats import py_schema


class Grant(py_schema.PySchemaElement):
    ddhkey: str
    consents: permissions.Consents


class Grants(py_schema.PySchemaElement):
    grants: list[Grant] = []

    @property
    def by_key(self):
        return {g.ddhkey: g.consents for g in self.grants}


class ConsentQuery(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest) -> Grants:
        """ obtain given and received consents and return them as a Grants object, which combines
            the key and its consents. 
        """

        op = req.access.ddhkey.split_at(req.key_split)[1]
        principal = req.access.principal
        assert principal

        match str(op).lower():
            case 'received':
                # We use the ConsentCache and convert to a list of Grant objects:
                grants = [Grant(ddhkey=str(k), consents=c)
                          for k, c in consentcache.ConsentCache.as_consents_for(principal).items()]

            case 'given':
                # we get all keys descending from the owner key:
                owner_key = keys.DDHkey(principal.id).ensure_rooted()
                node_keys = keydirectory.NodeRegistry.get_keys_with_prefix(owner_key)
                # get the consent nodes, and consents
                grants = []
                for cnode in await keydirectory.NodeRegistry.get_nodes_from_tuple_keys(node_keys, nodes.NodeSupports.consents, req.transaction):
                    assert cnode.consents
                    assert cnode.key
                    grants.append(Grant(ddhkey=str(cnode.key), consents=cnode.consents))

            case _:
                raise errors.NotFound(f"Selection {op} not found; must be 'received' or 'given'")
        r = Grants(grants=grants)
        return r

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.key: Grants.to_schema()}


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    # Consents.insert_as_schema(transaction, keys.DDHkeyGeneric('//org/ddh/consents'))
    attrs = dapp_attrs.SchemaProvider(references=[])
    cq = ConsentQuery(owner=principals.RootPrincipal, attrs=attrs, key=keys.DDHkeyVersioned0('//org/ddh/consents'))
    cq.register(session)


install()
