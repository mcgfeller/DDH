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

class AccessError(PermissionError):
    def __init__(self,text,consent=None):
        self.text = text

    def __str__(self):
        return self.text 

def get_schema(access : permissions.Access, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> typing.Optional[typing.Any]:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    formatted_schema = None # in case of not found. 
    snode,split = nodes.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.nschema) # get applicable schema nodes
    ok,consent,text = access.permitted()
    if not ok:
       return None
    
    if snode:
        schema = snode.get_sub_schema(access.ddhkey,split)
        if schema:
            formatted_schema = schema.format(schemaformat)
    return formatted_schema


def get_data(access : permissions.Access, q : typing.Optional[str] = None) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.
    """
    enode,split = nodes.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.execute)
    enode = typing.cast(nodes.ExecutableNode,enode)
    data = enode.execute(access.principal, q)
    return data