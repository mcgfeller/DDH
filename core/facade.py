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
from . import errors
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


    

def get_access(access : permissions.Access, session : sessions.Session, q : typing.Optional[str] = None, ) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.

        First we get the data (and consent), then we pass it to an enode if an enode is found.

    """
    data_node,d_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.data)
    if data_node:
        data_node = typing.cast(nodes.DataNode,data_node)
        topkey,remainder = access.ddhkey.split_at(d_key_split)
        data = data_node.read_data(remainder)
    else:
        data = {}

    # now for the enode:
    e_node,e_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey.without_owner(),nodes.NodeType.execute)
    e_node = typing.cast(nodes.ExecutableNode,e_node)
    # need to get owner of ressource, we need owner node and nodetuple for this
    nak = keydirectory.NodeRegistry.get_nodes(access.ddhkey)
    transaction = session.get_transaction(for_user=nak.owner.owner,create=True)
    transaction.accesses.append(access)
    if e_node:
        data = e_node.execute(access, e_key_split, data, q)
    return data

def put_access(access : permissions.Access, session : sessions.Session, data : pydantic.Json, q : typing.Optional[str] = None, ) -> typing.Any:
    """ Service utility to store data.
    """
    data_node,d_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeType.data)
    if not data_node:

        topkey,remainder = access.ddhkey.split_at(2)
        # there is no node, create it if owner asks for it:
        if topkey.owners == access.principal.id:
            data_node = nodes.DataNode(owner= access.principal,key=topkey)
            data_node.store(access) # put node into directory
            d_key_split = 0 # now this is the split
        else: # not owner, we simply say no access to this path
            raise errors.AccessError(f'not authorized to write to {topkey}')
    else:
        topkey,remainder = access.ddhkey.split_at(d_key_split)

    data_node = typing.cast(nodes.DataNode,data_node)
    
    if access.ddhkey.fork == keys.ForkType.data:
        data_node.insert(remainder,data)
    elif access.ddhkey.fork == keys.ForkType.consents:
        consents = permissions.Consents.parse_raw(data)
        data_node.update_consents(access, remainder,consents)
            

    

    # nodekey = access.ddhkey.without_owner()
    # enode,e_key_split = keydirectory.NodeRegistry.get_node(nodekey,nodes.NodeType.execute)
    # enode = typing.cast(nodes.ExecutableNode,enode)
    # # need to get owner of ressource, we need owner node and nodetuple for this
    # nak = keydirectory.NodeRegistry.get_nodes(access.ddhkey)
    # transaction = session.get_transaction(for_user=nak.owner.owner,create=True)
    # transaction.accesses.append(access)
    # if enode:
    #     data = enode.execute(access, key_split, q)
    # else:
    #     data = {}
    return data