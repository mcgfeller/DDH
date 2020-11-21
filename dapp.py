""" Support for DApps """
from __future__ import annotations
import core
import typing


class DApp(core.NoCopyBaseModel):
    

    owner : typing.ClassVar[core.Principal] 
    schemakey : typing.ClassVar[core.DDHkey] 

    @classmethod
    def bootstrap(cls) -> DApp:
        return cls()

    def startup(self)  -> core.Node:
        dnode = self.check_registry()
        return dnode

    def check_registry(self) -> core.Node:
        dnode = core.NodeRegistry[self.schemakey]
        if not dnode:
            # get a parent scheme to hook into
            upnode,split = core.NodeRegistry.get_node(self.schemakey,core.NodeType.nschema)
            pkey = self.schemakey.up()
            if not pkey:
                raise ValueError(f'{self.schemakey} key is too high {self!r}')
            parent = upnode.get_sub_schema(pkey,split)
            if not parent:
                raise ValueError(f'No parent schema found for {self!r} with {self.schemakey} at upnode {upnode}')
            s = self.get_schema() # obtain static schema
            dnode = core.DAppNode(owner=self.owner,schema=s)
            core.NodeRegistry[self.schemakey] = dnode
            # now insert our schema into the parent's:
            schemaref = core.SchemaReference.create_from_key(self.__class__.__name__,ddhkey=self.schemakey)
            parent.add_fields({self.schemakey[-1] : (schemaref,None)})
        return dnode 
    
    def get_schema(self) -> core.Schema:
        """ Obtain initial schema for DApp - this is stored in the Node and must be static. """
        raise NotImplementedError()
    


    