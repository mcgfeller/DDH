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
        dnode,split = core.NodeRegistry.get_node(self.schemakey,core.NodeType.nschema)
        if not dnode:
            s = self.get_schema()
            dnode = core.DAppNode(owner=self.owner,schema=s)
            core.NodeRegistry[self.schemakey] = dnode
        return dnode 
    
    def get_schema(self) -> core.Schema:
        raise NotImplementedError()
    


    