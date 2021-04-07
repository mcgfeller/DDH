
""" DDH Core Node Models """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions
from . import schemas



@enum.unique
class NodeType(str,enum.Enum):
    """ Types of Nodes, marked by presence of attribute corresponding with enum value """

    owner = 'owner'
    nschema = 'nschema'
    consents = 'consents'
    data = 'data'
    execute = 'execute'

    def __repr__(self): return self.value


class Node(NoCopyBaseModel):

    types: set[NodeType] = set() # all supported type, will be filled by init unless given
    owner: permissions.Principal
    consents : typing.Optional[permissions.Consents] = None
    nschema : typing.Optional[schemas.Schema] =  pydantic.Field(alias='schema')
    key : typing.Optional[keys.DDHkey] = None

    def __init__(self,**data):
        """ .types will be filled based on attributes that are not Falsy """
        super().__init__(**data)
        if not self.types:
            self.types = {t for t in NodeType if getattr(self,t.value,None)}
        return

    def __str__(self):
        """ short representation """
        return f'Node(types={self.types!s},key={self.key!s},owner={self.owner.id})'


    def get_sub_schema(self, ddhkey: keys.DDHkey,split: int, schema_type : str = 'json') -> typing.Optional[schemas.Schema]:
        """ return schema based on ddhkey and split """
        s = typing.cast(schemas.Schema,self.nschema)
        s = s.obtain(ddhkey,split)
        return s

    @property
    def owners(self) -> list[permissions.Principal]:
        """ get one or multiple owners """
        return [self.owner]


        


from . import keys # avoid circle
Node.update_forward_refs() # Now Node is known, update before it's derived

class MultiOwnerNode(Node):

    all_owners : list[permissions.Principal]
    consents : typing.Union[permissions.Consents,permissions.MultiOwnerConsents,None] = None

    def __init__(self,**data):
        data['owner'] = data.get('all_owners',[None])[0] # first owner, will complain in super
        super().__init__(**data)
        if isinstance(self.consents,permissions.Consents): # Convert Consents into MultiOwnerConsents:
            self.consents = permissions.MultiOwnerConsents(consents_by_owner={self.owner: self.consents})

        return

    @property
    def owners(self) -> list[permissions.Principal]:
        """ get one or multiple owners """
        return self.all_owners


class ExecutableNode(Node):
    """ A node that provides for execution capabilities """

    type: typing.ClassVar[NodeType] = NodeType.execute

    @abstractmethod
    def execute(self, access : permissions.Access, key_split : int, q : typing.Optional[str] = None):
        return {}

class DelegatedExecutableNode(ExecutableNode):
    """ A node that delegates executable methods to DApps """

    executors : list = []

    def execute(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        for executor in self.executors:
            d = executor.get_and_transform(access,key_split, q)
        return d


class _NodeRegistry:
    """ Preliminary holder of nodes 
        Note that Nodes are held per NodeType, duplicating them as required 
        for easy lookup by NodeType.
    """

    nodes_by_key : dict[tuple,dict[NodeType,Node]] # by key, then by NodeTypes

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : keys.DDHkey, node: Node):
        """ Store the node, with a reference per NodeType """
        node.key = key
        by_nodetype = self.nodes_by_key.setdefault(key.key,{}) 
        for nt in node.types:
            by_nodetype[nt] = node
        return 


    def __getitem__(self,key : keys.DDHkey) -> dict[NodeType,Node]:
        return self.nodes_by_key.get(key.key,{}) 

    def get_next_node(self,key : typing.Optional[keys.DDHkey], type: NodeType) -> typing.Iterator[typing.Tuple[Node,int]]:
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

    def get_node(self,key : keys.DDHkey,type : NodeType) -> typing.Tuple[typing.Optional[Node],int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        node,split = next(( (node,split) for node,split in self.get_next_node(key, type) ),(None,-1))
        return node,split
    

NodeRegistry = _NodeRegistry()
