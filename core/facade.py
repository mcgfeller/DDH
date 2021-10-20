""" rovides a Facade from Core to Frontend """

from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc
import json


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions, keys, schemas, nodes, keydirectory, transactions, errors
from frontend import sessions

def get_consent_node(ddhkey: keys.DDHkey, node : nodes.Node, support: nodes.NodeSupports, transaction : transactions.Transaction) -> typing.Optional[nodes.Node]:
    """ get consents, from current node or from its parent """
    cnode = node
    while True:
        if cnode.consents:
            return cnode
        else:
            ddhkey = ddhkey.up()
            if ddhkey:
                cnode,split = keydirectory.NodeRegistry.get_node(ddhkey,support,transaction)
            else:
                return None
    


def get_schema(access : permissions.Access, session : sessions.Session, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> typing.Optional[typing.Any]:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)
    formatted_schema = None # in case of not found. 
    snode,split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeSupports.schema,transaction) # get applicable schema nodes
   
    if snode:
        cnode = get_consent_node(access.ddhkey,snode,nodes.NodeSupports.schema,transaction)
        ok,consent,text = access.permitted(cnode)
        if ok:
            schema = snode.get_sub_schema(access.ddhkey,split)
            if schema:
                formatted_schema = schema.format(schemaformat)
    return formatted_schema


    

def get_access(access : permissions.Access, session : sessions.Session, q : typing.Optional[str] = None, ) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.

        First we get the data (and consent), then we pass it to an enode if an enode is found.

    """
    # if we ask for schema, we don't need a transaction:
    if access.ddhkey.fork == keys.ForkType.schema:
        return get_schema(access, schemaformat=schemas.SchemaFormat.json)
    else: # data or consent
        transaction = session.get_or_create_transaction(for_user=access.principal)
        transaction.add_and_validate(access)

        # get data node first
        data_node,d_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeSupports.data,transaction)
        if data_node:
            cnode = get_consent_node(access.ddhkey,data_node,nodes.NodeSupports.data,transaction)
            ok,consent,text = access.permitted(cnode)
            if not ok:
                raise errors.AccessError(text)
            
        if access.ddhkey.fork == keys.ForkType.consents:
            return data_node.consents
        else:
            if data_node:
                data_node = data_node.ensure_loaded(transaction)
                data_node = typing.cast(nodes.DataNode,data_node)
                topkey,remainder = access.ddhkey.split_at(d_key_split)
                data = data_node.execute(nodes.Ops.get,access, transaction, d_key_split, None, q)
            else:
                data = {}

            # now for the enode:
            e_node,e_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey.without_owner(),nodes.NodeSupports.execute,transaction)
            if e_node:
                e_node = e_node.ensure_loaded(transaction)
                e_node = typing.cast(nodes.ExecutableNode,e_node)
                data = e_node.execute(nodes.Ops.get,access, transaction, e_key_split, data, q)
            return data

def put_access(access : permissions.Access, session : sessions.Session, data : pydantic.Json, q : typing.Optional[str] = None, ) -> typing.Any:
    """ Service utility to store data.
        
    """
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)

    data_node,d_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey,nodes.NodeSupports.data,transaction)
    if not data_node:

        topkey,remainder = access.ddhkey.split_at(2)
        # there is no node, create it if owner asks for it:
        if topkey.owners == access.principal.id:
            data_node = nodes.DataNode(owner= access.principal,key=topkey)
            data_node.store(transaction) # put node into directory
            d_key_split = 0 # now this is the split
        else: # not owner, we simply say no access to this path
            raise errors.AccessError(f'not authorized to write to {topkey}')
    else:
        data_node = data_node.ensure_loaded(transaction)
        topkey,remainder = access.ddhkey.split_at(d_key_split)

    data_node = typing.cast(nodes.DataNode,data_node)
    
    if access.ddhkey.fork == keys.ForkType.data:
        data = json.loads(data) # make dict
        # first e_node to transform data:
        e_node,e_key_split = keydirectory.NodeRegistry.get_node(access.ddhkey.without_owner(),nodes.NodeSupports.execute,transaction)
        if e_node:
            e_node = e_node.ensure_loaded(transaction)
            e_node = typing.cast(nodes.ExecutableNode,e_node)
            data = e_node.execute(nodes.Ops.put,access, transaction, e_key_split, data, q) 
        if data:
            data_node.execute(nodes.Ops.put,access, transaction, d_key_split, data, q)

    elif access.ddhkey.fork == keys.ForkType.consents:
        consents = permissions.Consents.parse_raw(data)
        data_node.update_consents(access, transaction, remainder,consents)
    return data