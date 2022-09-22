""" Proxy representing DApps in runner  """
from __future__ import annotations
import enum
import typing
import pydantic
import pydantic.json
import httpx
import json

import logging

logger = logging.getLogger(__name__)

from utils import utils
# from frontend import sessions
from core import keys,permissions,schemas,nodes,keydirectory,policies,errors,transactions,principals,relationships,pillars,common_ids,dapp_attrs,versions
from utils.pydantic_utils import NoCopyBaseModel




class DAppProxy(NoCopyBaseModel):
    """ This is a Proxy running in the Walled Garden of DApps (running in their own microservices) """

    class Config:
        arbitrary_types_allowed = True

    id: str
    running :  dapp_attrs.RunningDApp
    attrs : dapp_attrs.DApp
    client : httpx.AsyncClient
    schemas: dict[keys.DDHkey,schemas.AbstractSchema] = {}


    async def initialize(self,session,pillars : dict):
        if True: # self.running.schema_version > versions._UnspecifiedVersion:
            j = await(self.client.get('/schemas'))
            j.raise_for_status()
            js = j.json()
            self.schemas =  {keys.DDHkey(k):schemas.create_schema(s,sa) for k,(sa,s) in js.items()}
            print('initialize',self.schemas)
            schemaNetwork : pillars.SchemaNetworkClass = pillars['SchemaNetwork']
            dnodes = self.register_schema(session)
            self.register_references(session,schemaNetwork)
        return



    def register_references(self,session, schemaNetwork : pillars.SchemaNetworkClass):
        """ We register: 
            - DAppNode where our DApp provides or transforms into a DDHkey
            - DApp as SchemaNetwork node, with edges to provides, transforms and requires 
        """
        transaction = session.get_or_create_transaction()
        attrs = self.attrs
        schemaNetwork.network.add_node(attrs,id=attrs.id,type='dapp',
            cost=attrs.estimated_cost(),availability_user_dependent=attrs.availability_user_dependent())
        for ref in attrs.references:
            # we want node attributes of, so get the node: 
            snode,split = keydirectory.NodeRegistry.get_node(ref.target,nodes.NodeSupports.schema,transaction) # get applicable schema node for attributes
            sa = snode.schemas.current_schema.schema_attributes
            schemaNetwork.network.add_node(ref.target,id=str(ref.target),type='schema',requires=sa.requires)
            if ref.relation == relationships.Relation.provides:
                schemaNetwork.network.add_edge(attrs,ref.target,type='provides',weight=attrs.get_weight())
            elif ref.relation == relationships.Relation.requires:
                schemaNetwork.network.add_edge(ref.target,attrs,type='requires')
        if self.attrs.transforms_into:
            self.register_transform(self.attrs.transforms_into)
        return




    def register_schema(self,session) -> list[nodes.Node]:
        """ We register: 
            - SchemaNode for the Schemas our node provides

        """
        transaction = session.get_or_create_transaction()
        
        dnodes = []
        for schemakey,schema in self.schemas.items():
            dnode = keydirectory.NodeRegistry[schemakey].get(nodes.NodeSupports.schema) # need exact location, not up the tree
            if dnode:
                dnode = typing.cast(DAppNode,dnode.ensure_loaded(transaction))
                dnode.add_schema(schema)
            else:
                # create dnode with our schema:
                dnode = DAppNode(owner=self.attrs.owner,dapp=self,consents=schemas.AbstractSchema.get_schema_consents())
                dnode.add_schema(schema) # TODO: Separate DAppNode and SchemaNode
                # hook into parent schema:
                schemas.AbstractSchema.insert_schema(self.attrs.id, schemakey,transaction)
                keydirectory.NodeRegistry[schemakey] = dnode
            dnodes.append(dnode)
        return dnodes 

    def register_transform(self,ddhkey : keys.DDHkey):
        de_node = keydirectory.NodeRegistry[ddhkey].get(nodes.NodeSupports.execute)
        if not de_node:
            de_node = nodes.DelegatedExecutableNode(owner=self.attrs.owner)
            de_node.executors.append(self)
            keydirectory.NodeRegistry[ddhkey] = de_node
        return
    

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        data = await self.client.post('execute',data=req.json())
        data.raise_for_status()
        return data.json()

    async def get_and_transform(self, req: dapp_attrs.ExecuteRequest):
        data = await self.client.post('get_and_transform',data=req.json())
        data.raise_for_status()
        return data.json()




class DAppManagerClass(NoCopyBaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById : dict[principals.DAppId,DAppProxy] = {} # registry of DApps

    async def register(self,request,session,running_dapp: dapp_attrs.RunningDApp):
        client = httpx.AsyncClient(base_url=running_dapp.location)
        j = await client.get('/app_info') # get dict of dapp_attrs, one microservice may return multiple DApps
        j.raise_for_status()
        dattrs = j.json()
        for id,attrs in dattrs.items():
            attrs = dapp_attrs.DApp(**attrs)
            proxy = DAppProxy(id=id,running=running_dapp,attrs=attrs,client=client)
            await proxy.initialize(session,pillars.Pillars) # initialize gets schema and registers everything
            self.DAppsById[typing.cast(principals.DAppId, id)] =  proxy
        return 

    def bootstrap(self, pillars:dict) :
        pillars['SchemaNetwork'].complete_graph() # TODO: Call it after each register?
        # pillars['SchemaNetwork'].plot(layout='shell_layout')
        return





DAppManager = DAppManagerClass()
    


class DAppNode(nodes.ExecutableNode):
    """ node managed by a DApp """
    dapp : DAppProxy

    def __hash__(self):
        return hash(self.dapp)

    def __eq__(self,other):
        return (self.dapp == other.dapp) if isinstance(other,DAppNode) else False



    async def execute(self, req: dapp_attrs.ExecuteRequest):
        r = await self.dapp.execute(req)
        return r
 
