""" Schemas //org/ddh/consents, providing an API against principaled information about DDH itself: Consents
"""


import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, keydirectory, errors, permissions, common_ids, dapp_attrs, consentcache, nodes
from utils import utils
from frontend import sessions
from backend import queues
from schema_formats import py_schema


class Grants(py_schema.PySchemaElement):
    """ Grants (with Schema) """
    grants: dict[str, permissions.Consents] = {}


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
                # We use the ConsentCache and convert to a dict:
                grants = {str(k): c for k, c in consentcache.ConsentCache.as_consents_for(principal).items()}

            case 'given':
                # we get all keys descending from the owner key:
                owner_key = keys.DDHkey(principal.id).ensure_rooted()
                node_keys = keydirectory.NodeRegistry.get_keys_with_prefix(owner_key)
                # get the consent nodes, and consents
                cnodes = await keydirectory.NodeRegistry.get_nodes_from_tuple_keys(node_keys, nodes.NodeSupports.consents, req.transaction)
                grants = {str(cnode.key.for_consent_grants()): cnode.consents for cnode in cnodes}

            case _:
                raise errors.NotFound(f"Selection {op} not found; must be 'received' or 'given'")
        r = Grants(grants=grants)
        return r

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp, mark it is subscribable """
        s = Grants.to_schema()
        s.schema_attributes.subscribable = True
        return {self.key: s}


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    attrs = dapp_attrs.SchemaProvider()
    cq = ConsentQuery(owner=principals.RootPrincipal, attrs=attrs,
                      key=keys.DDHkeyVersioned0('//org/ddh/consents'), subscribable=True)
    cq.register(session)


install()
