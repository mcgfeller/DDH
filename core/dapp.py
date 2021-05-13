""" Support for DApps """
from __future__ import annotations
from abc import abstractmethod
import typing

from core import keys,permissions,schemas,nodes,keydirectory,policies,errors
from utils.pydantic_utils import NoCopyBaseModel,pyright_check


@pyright_check
class DApp(NoCopyBaseModel):
    
    owner : typing.ClassVar[permissions.Principal] 
    schemakey : typing.ClassVar[keys.DDHkey] 
    policy: policies.Policy = policies.EmptyPolicy
    

    @property
    def id(self) -> permissions.DAppId:
        """ Default DAppId is class name """
        return typing.cast(permissions.DAppId,self.__class__.__name__) 

    @classmethod
    def bootstrap(cls) -> DApp:
        return cls()

    def startup(self)  -> nodes.Node:
        dnode = self.check_registry()
        return dnode

    def check_registry(self) -> nodes.Node:
        dnode = keydirectory.NodeRegistry[self.schemakey].get(nodes.NodeType.nschema)
        if not dnode:
            # get a parent scheme to hook into
            upnode,split = keydirectory.NodeRegistry.get_node(self.schemakey,nodes.NodeType.nschema)
            pkey = self.schemakey.up()
            if not pkey:
                raise ValueError(f'{self.schemakey} key is too high {self!r}')
            parent = upnode.get_sub_schema(pkey,split)
            if not parent:
                raise ValueError(f'No parent schema found for {self!r} with {self.schemakey} at upnode {upnode}')
            schema = self.get_schema() # obtain static schema
            dnode = DAppNode(owner=self.owner,schema=schema,dapp=self)
            keydirectory.NodeRegistry[self.schemakey] = dnode
            # now insert our schema into the parent's:
            schemaref = schemas.SchemaReference.create_from_key(self.__class__.__name__,ddhkey=self.schemakey)
            parent.add_fields({self.schemakey[-1] : (schemaref,None)})
        return dnode 
    
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp - this is stored in the Node and must be static. """
        raise errors.SubClass

    @abstractmethod
    def execute(self, access : permissions.Access, key_split : int, q : typing.Optional[str] = None):
        return  {}
    

@pyright_check
class DAppNode(nodes.ExecutableNode):
    """ node managed by a DApp """
    dapp : DApp


    def execute(self, access : permissions.Access, key_split : int, q : typing.Optional[str] = None):
        r = self.dapp.execute(access,key_split, q)
        return r
 
