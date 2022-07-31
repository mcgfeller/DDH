""" Proxy representing DApps in runner  """
from __future__ import annotations
from abc import abstractmethod
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
            js = j.json()

            self.schemas = {keys.DDHkey(k):schemas.JsonSchema(json_schema=json.dumps(s),schema_attributes=sa) for k,(sa,s) in js.items()}
            print('initialize',self.schemas)
            schemaNetwork : pillars.SchemaNetworkClass = pillars['SchemaNetwork']
            dnodes = self.register_schema(session)
            self.register_references(session,schemaNetwork)
        return



    def startup(self,session,pillars : dict)  -> list[nodes.Node]:
        schemaNetwork : pillars.SchemaNetworkClass = pillars['SchemaNetwork']
        dnodes = self.register_schema(session)
        self.register_references(session,schemaNetwork)

        return dnodes

    def register_references(self,session, schemaNetwork : pillars.SchemaNetworkClass):
        transaction = session.get_or_create_transaction()
        attrs = self.attrs
        schemaNetwork.network.add_node(attrs,id=attrs.id,type='dapp',
            cost=attrs.estimated_cost(),availability_user_dependent=attrs.availability_user_dependent())
        for ref in attrs.references:
            # we want node attributes of, so get the node: 
            snode,split = keydirectory.NodeRegistry.get_node(ref.target,nodes.NodeSupports.schema,transaction) # get applicable schema node for attributes
            sa = snode.nschema.schema_attributes
            schemaNetwork.network.add_node(ref.target,id=str(ref.target),type='schema',requires=sa.requires)
            if ref.relation == relationships.Relation.provides:
                schemaNetwork.network.add_edge(attrs,ref.target,type='provides',weight=attrs.get_weight())
            elif ref.relation == relationships.Relation.requires:
                schemaNetwork.network.add_edge(ref.target,attrs,type='requires')
        return




    def register_schema(self,session) -> list[nodes.Node]:
        transaction = session.get_or_create_transaction()
        
        dnodes = []
        for schemakey,schema in self.schemas.items():
            dnode = keydirectory.NodeRegistry[schemakey].get(nodes.NodeSupports.schema) # need exact location, not up the tree
            if dnode:
                dnode = typing.cast(DAppNode,dnode.ensure_loaded(transaction))
                dnode.add_schema_version(schema)
            else:
                # create dnode with our schema:
                dnode = DAppNode(owner=self.attrs.owner,schema=schema,dapp=self,consents=schemas.AbstractSchema.get_schema_consents())
                

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
    

    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        return  data



class DAppManagerClass(NoCopyBaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById : dict[principals.DAppId,DAppProxy] = {} # registry of DApps

    # async def register(self,session,running_dapp: dapp_attrs.RunningDApp):
    #     # transaction = session.get_or_create_transaction()
    #     client = httpx.AsyncClient(base_url=running_dapp.location)
    #     proxies = await DAppProxy.from_running(running_dapp)
    #     self.DAppsById.update({typing.cast(principals.DAppId, proxy.running.id) : proxy for proxy in proxies})
    #     print('Connect',self,session,running_dapp)

    async def register(self,request,session,running_dapp: dapp_attrs.RunningDApp):
        client = httpx.AsyncClient(base_url=running_dapp.location)
        j = await client.get('/app_info')
        dattrs = j.json()
        for id,attrs in dattrs.items():
            attrs = dapp_attrs.DApp(**attrs)
            proxy = DAppProxy(id=id,running=running_dapp,attrs=attrs,client=client)
            await proxy.initialize(session,pillars.Pillars)
            self.DAppsById[typing.cast(principals.DAppId, id)] =  proxy
        return 

    def bootstrap(self, pillars:dict) :
        # session = sessions.get_system_session()
        # for module in import_modules.importAllSubPackages(DApps,raiseError=False):
        #     assert module
        #     classname = module.__name__.split('.')[-1]
        #     cls = getattr(module,classname,None) # class must have same name as module
        #     if not cls:
        #         logger.error(f'DApp module {module.__name__} has no DApp class named {classname}.')
        #     else:
        #         try:
        #             dapps = cls.bootstrap(session,pillars) # one class may generate multiple DApps
        #         except Exception as e:
        #             logger.error(f'DApp {cls.__name__} bootstrap error: {e}')
        #         else:
        #             dapps = utils.ensureTuple(dapps)
        #             for dapp in dapps:
        #                 self.DAppsById[dapp.id] = dapp
        #                 try:
        #                     dnode = dapp.startup(session,pillars)
        #                     logger.info(f'DApp {dapp!r} initialized at {dnode!s}.')
        #                 except Exception as e:
        #                     logger.error(f'DApp {dapp!r} startup error: {e}',exc_info=True)
        #                     raise

        # pillars['SchemaNetwork'].complete_graph()
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



    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        r = self.dapp.execute(op,access,transaction, key_split, data, q)
        return r
 
