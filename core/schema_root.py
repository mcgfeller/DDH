""" Root of DDH Schemas, defines top levels down to where DApps link in """

from __future__ import annotations
import pydantic 
import datetime
import typing
import logging

from core import keys,permissions,schemas,nodes,dapp
logger = logging.getLogger(__name__)

def check_registry() -> nodes.Node:
    """ Register root schema at root node. 
        This is preliminary, as the schema is hard-coded.
    """
    root = keys.DDHkey(keys.DDHkey.Root)
    root_node,split = nodes.NodeRegistry.get_node(root,nodes.NodeType.nschema)
    if not root_node:
        schema = build_schema(keys.DDHkey(key="/ddh/shopping/stores")) # obtain static schema
        # for now, give schema read access to everybody
        consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[permissions.AllPrincipal],withModes={permissions.AccessMode.schema_read})]) 
        root_node = nodes.Node(owner=permissions.RootPrincipal,schema=schema,consents=consents)
        nodes.NodeRegistry[root] = root_node
    logger.info('Schema Root built')
    return root_node 

def build_schema(ddhkey : keys.DDHkey):
    elements = {}
    s = None
    for k in ddhkey[::-1]: # loop backwards
        if k is keys.DDHkey.Root: k = '__root__'
        s = pydantic.create_model(k, __base__=schemas.SchemaElement, **elements)
        elements = {k:(s,None)}
    return schemas.PySchema(schema_element=s)

check_registry()
