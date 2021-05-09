
""" Directory mapping keys to nodes """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import datetime
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions
from . import schemas
from . import nodes
from . import keys



class NodeAtKey:
    def __init__(self,key: keys.DDHkey,**d):
        self.key = key
        self.__dict__.update(d)




class _NodeRegistry:
    """ Preliminary holder of nodes 
        Note that Nodes are held per NodeType, duplicating them as required 
        for easy lookup by NodeType.
    """

    nodes_by_key : dict[tuple,dict[nodes.NodeType,nodes.Node]] # by key, then by NodeTypes

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : keys.DDHkey, node: nodes.Node):
        """ Store the node, with a reference per NodeType """
        node.key = key
        by_nodetype = self.nodes_by_key.setdefault(key.key,{}) 
        for nt in node.types:
            by_nodetype[nt] = node
        return 


    def __getitem__(self,key : keys.DDHkey) -> dict[nodes.NodeType,nodes.Node]:
        return self.nodes_by_key.get(key.key,{}) 

    def get_next_node(self,key : typing.Optional[keys.DDHkey], type: nodes.NodeType) -> typing.Iterator[typing.Tuple[nodes.Node,int]]:
        """ Generator getting next node walking up the tree from key.
            Also indicates at which point the keys.DDHkey is to be split so the first part is the
            path leading to the Node, the 2nd the rest. 
            """
        split = len(key.key) # where to split: counting backwards from the end. 
        while key:
            nodes =  self[key]
            key = key.up() 
            split -= 1
            node = nodes.get(type) # required type?
            if node:
                yield node,split+1
        else:
            return

    def get_node(self,key : keys.DDHkey,type : nodes.NodeType) -> typing.Tuple[typing.Optional[nodes.Node],int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        node,split = next(( (node,split) for node,split in self.get_next_node(key, type) ),(None,-1))
        return node,split


    def get_nodes(self,key : keys.DDHkey, types : set[nodes.NodeType] = set()) -> NodeAtKey:
        if not types:
            types = {nodes.NodeType.data,nodes.NodeType.nschema, nodes.NodeType.consents}
        d = {}
        for type in types:
            n,n_split = self.get_node(key,type)
            d[type.value] = n
            d[type.value+'_split'] = n_split
        nak = NodeAtKey(key=key,**d)
        return nak

    

NodeRegistry = _NodeRegistry()