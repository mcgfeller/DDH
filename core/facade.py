""" rovides a Facade from Core to Frontend """

from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions
from . import keys
from . import schemas
from . import nodes
from . import keydirectory
from frontend import sessions

def get_schema(access : permissions.Access, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> typing.Optional[typing.Any]:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    formatted_schema = None # in case of not found. 
    snode,split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.nschema) # get applicable schema nodes
    ok,consent,text = access.permitted()
    if not ok:
       return None
    
    if snode:
        schema = snode.get_sub_schema(access.ddhkey,split)
        if schema:
            formatted_schema = schema.format(schemaformat)
    return formatted_schema


    

def perform_access(access : permissions.Access, session : sessions.Session, q : typing.Optional[str] = None, ) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.
    """

    enode,key_split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.execute)
    enode = typing.cast(nodes.ExecutableNode,enode)
    # need to get owner of ressource, we need owner node and nodetuple for this
    # transaction = session.get_transaction(for_user)
    if enode:
        data = enode.execute(access, key_split, q)
    else:
        data = {}
    return data