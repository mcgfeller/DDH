""" Support for DApps """

import core
import typing

class DApp(core.NoCopyBaseModel):
    

    owner : core.Principal
    schemakey : core.DDHkey

    def startup(self):
        self.check_registry()

    def check_registry(self):
        snode = core.NodeRegistry.get_node(self.schemakey,core.NodeType.nschema)
        if not snode:
            s = self.get_schema()
            snode = core.DAppNode(owner=self.owner,nschema=s)
            core.NodeRegistry[self.schemakey] = snode
        return
    
    def get_schema(self) -> core.Schema:
        raise NotImplementedError()





class DAppStore(core.NoCopyBaseModel):

    apps : typing.Dict[str,DApp] = {}
    
