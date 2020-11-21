
""" DDH Core Node Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions
# 
from . import schemas



@enum.unique
class NodeType(str,enum.Enum):
    """ Types of Nodes, marked by presence of attribute corresponding with enum value """

    owner = 'owner'
    nschema = 'nschema'
    consents = 'consents'
    data = 'data'
    execute = 'execute'


class Node(NoCopyBaseModel):

    owner: permissions.Principal
    consents : typing.Optional[permissions.Consents] = None
    nschema : typing.Optional[schemas.Schema] =  pydantic.Field(alias='schema')
    key : typing.Optional[keys.DDHkey] = None

    def __str__(self):
        """ short representation """
        return f'Node(key={self.key!s},owner={self.owner.id})'


    def get_sub_schema(self, ddhkey: keys.DDHkey,split: int, schema_type : str = 'json') -> typing.Optional[schemas.Schema]:
        """ return schema based on ddhkey and split """
        s = typing.cast(schemas.Schema,self.nschema)
        s = s.obtain(ddhkey,split)
        return s


from . import keys # avoid circle
Node.update_forward_refs() # Now Node is known, update before it's derived

class ExecutableNode(Node):
    def execute(self,  user: permissions.Principal, q : typing.Optional[str] = None):
        return {}


class DAppNode(ExecutableNode):
    """ node managed by a DApp """
    ...

class StorageNode(ExecutableNode):
    """ node with storage on DDH """
    ...



class _NodeRegistry:
    """ Preliminary holder of nodes """

    nodes_by_key : dict[tuple,Node]

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : keys.DDHkey, node: Node):
        self.nodes_by_key[key.key] = node
        node.key = key

    def __getitem__(self,key : keys.DDHkey) -> typing.Optional[Node]:
        return self.nodes_by_key.get(key.key,None) 

    def get_next_node(self,key : typing.Optional[keys.DDHkey]) -> typing.Iterator[typing.Tuple[Node,int]]:
        """ Generating getting next node walking up the tree from key.
            Also indicates at which point the keys.DDHkey is to be split so the first part is the
            path leading to the Node, the 2nd the rest. 
            """
        split = len(key.key) # where to split: counting backwards from the end. 
        while key:
            node =  self[key]
            key = key.up() 
            split -= 1
            if node:
                yield node,split+1
        else:
            return

    def get_node(self,key : keys.DDHkey,node_type : NodeType) -> typing.Tuple[typing.Optional[Node],int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        node,split = next(( (node,split) for node,split in self.get_next_node(key) if getattr(node,node_type.value,None)),(None,-1))
        return node,split
    

NodeRegistry = _NodeRegistry()
