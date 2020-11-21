""" Support for DApps """
from __future__ import annotations
import typing

from core import keys,permissions,schemas,nodes
from utils.pydantic_utils import NoCopyBaseModel

class DApp(NoCopyBaseModel):
    

    owner : typing.ClassVar[permissions.Principal] 
    schemakey : typing.ClassVar[keys.DDHkey] 

    @classmethod
    def bootstrap(cls) -> DApp:
        return cls()

    def startup(self)  -> nodes.Node:
        dnode = self.check_registry()
        return dnode

    def check_registry(self) -> nodes.Node:
        dnode = nodes.NodeRegistry[self.schemakey]
        if not dnode:
            # get a parent scheme to hook into
            upnode,split = nodes.NodeRegistry.get_node(self.schemakey,nodes.NodeType.nschema)
            pkey = self.schemakey.up()
            if not pkey:
                raise ValueError(f'{self.schemakey} key is too high {self!r}')
            parent = upnode.get_sub_schema(pkey,split)
            if not parent:
                raise ValueError(f'No parent schema found for {self!r} with {self.schemakey} at upnode {upnode}')
            schema = self.get_schema() # obtain static schema
            dnode = nodes.DAppNode(owner=self.owner,schema=schema)
            nodes.NodeRegistry[self.schemakey] = dnode
            # now insert our schema into the parent's:
            schemaref = schemas.SchemaReference.create_from_key(self.__class__.__name__,ddhkey=self.schemakey)
            parent.add_fields({self.schemakey[-1] : (schemaref,None)})
        return dnode 
    
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp - this is stored in the Node and must be static. """
        raise NotImplementedError()
    


    