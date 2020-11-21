""" Root of DDH Schemas, defines top levels down to where DApps link in """

from __future__ import annotations
import pydantic 
import datetime
import typing
import logging

from core import core
logger = logging.getLogger(__name__)

def check_registry() -> core.Node:
    root = core.DDHkey(core.DDHkey.Root)
    dnode,split = core.NodeRegistry.get_node(root,core.NodeType.nschema)
    if not dnode:
        schema = build_schema(core.DDHkey(key="/ddh/shopping/stores")) # obtain static schema
        # for now, give schema read access to everybody
        consents = core.Consents(consents=[core.Consent(grantedTo=[core.AllPrincipal],withModes={core.AccessMode.schema_read})]) 
        dnode = core.DAppNode(owner=core.RootPrincipal,schema=schema,consents=consents)
        core.NodeRegistry[root] = dnode
    logger.info('Schema Registry built')
    return dnode 

def build_schema(key : core.DDHkey):
    elements = {}
    s = None
    for k in key[::-1]: # loop backwards
        if k is core.DDHkey.Root: k = '__root__'
        s = pydantic.create_model(k, __base__=core.SchemaElement, **elements)
        elements = {k:(s,None)}
    return core.PySchema(schema_element=s)

check_registry()
