""" Proxy representing DApps in runner  """
from __future__ import annotations
import enum
import typing
import pydantic
import pydantic.json
import httpx
import json
import asyncio

import logging

logger = logging.getLogger(__name__)

# from frontend import sessions
from core import keys, schemas, nodes, keydirectory, errors, transactions, principals, relationships, schema_network, dapp_attrs
from utils.pydantic_utils import DDHbaseModel


class DAppProxy(DDHbaseModel):
    """ This is a Proxy running in the Walled Garden of DApps (running in their own microservices) """

    class Config:
        arbitrary_types_allowed = True

    id: str
    running:  dapp_attrs.RunningDApp
    attrs: dapp_attrs.DApp
    # TODO: Move .client to running.client
    client: httpx.AsyncClient = pydantic.Field(exclude=True)  # private and not json-able
    schemas: dict[keys.DDHkeyVersioned, schemas.AbstractSchema] = {}

    async def initialize(self, session, pillars: dict):
        if True:  # self.running.schema_version > versions._UnspecifiedVersion:
            j = await(self.client.get('/schemas'))
            j.raise_for_status()
            js = j.json()
            self.schemas = {keys.DDHkeyVersioned(k): schemas.AbstractSchema.create_schema(s, sf, sa)
                            for k, (sa, sf, s) in js.items()}
            self.register_schemas(session)
            self.register_references(session, schemas.SchemaNetwork)
            schemas.SchemaNetwork.valid.invalidate()  # finished
        return

    def register_schemas(self, session) -> list[nodes.Node]:
        """ We register: 
            - SchemaNode for the Schemas our node provides, including transformed-into keys.

        """
        transaction = session.get_or_create_transaction()

        snodes = []
        for schemakey, schema in self.schemas.items():
            snode = self.register_schema(schemakey, schema, self.attrs.owner, transaction)

            #
            snodes.append(snode)
        return snodes

    @staticmethod
    def register_schema(schemakey: keys.DDHkeyVersioned, schema: schemas.AbstractSchema, owner: principals.Principal, transaction):
        """ register a single schema in its Schema Node, creating one if necessary. 
            staticmethod so it can be called by test fixtures. 
        """
        genkey = schemakey.without_variant_version()
        snode = keydirectory.NodeRegistry[genkey].get(
            nodes.NodeSupports.schema)  # need exact location, not up the tree
        # hook into parent schema:
        parent, split = schemas.AbstractSchema.get_parent_schema(transaction, genkey)
        # inherit transformers:
        schema.schema_attributes.transformers = parent.schema_attributes.transformers.merge(
            schema.schema_attributes.transformers)

        if snode:
            # TODO:#33: Schema Nodes don't need .ensure_loaded now, but should be reinserted once they're async
            snode = typing.cast(nodes.SchemaNode, snode)
            snode.add_schema(schema)
        else:
            # create snode with our schema:
            snode = nodes.SchemaNode(owner=owner, consents=schemas.AbstractSchema.get_schema_consents())
            keydirectory.NodeRegistry[genkey] = snode
            snode.add_schema(schema)

            parent.insert_schema_ref(transaction, genkey, split)
        return snode

    def register_references(self, session, schema_network: schema_network.SchemaNetworkClass):
        """ We register: 
            - DAppNode where our DApp provides or transforms into a DDHkeyVersioned
            - DApp as SchemaNetwork node, with edges to provides, transforms and requires 
        """
        transaction = session.get_or_create_transaction()

        attrs = self.attrs
        dnode = DAppNode(owner=attrs.owner, dapp=self, consents=schemas.AbstractSchema.get_schema_consents())
        schema_network.add_dapp(attrs)

        for ref in attrs.references:
            # # we want node attributes of, so get the node:
            # snode, split = keydirectory.NodeRegistry.get_node(
            #     ref.target, nodes.NodeSupports.schema, transaction)  # get transformer schema node for attributes
            # sa = snode.schemas.get().schema_attributes
            # schema_network.add_schema_node(ref.target, sa)
            target = ref.target.ens()
            if ref.relation == relationships.Relation.provides:
                target = keys.DDHkeyVersioned0.cast(target)
                schema_network.add_schema_vv(target.without_variant_version(), target)
                schema_network.add_edge(target, attrs, type='provided by', weight=attrs.get_weight())
                # register our node as a provider for (or transformer into) the key:
                keydirectory.NodeRegistry[ref.target] = dnode
            elif ref.relation == relationships.Relation.requires:
                target = keys.DDHkeyRange.cast(target)
                schema_network.add_schema_range(target)
                schema_network.add_edge(attrs, target, type='requires')
        schema_network.valid.invalidate()  # we have modified the network
        return

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        """ forward execution request to DApp microservice """
        resp = await self.client.post('execute', data=req.json())
        print(f'*** DAppProxy.execute {resp=}  {resp.text=}')
        errors.DAppError.raise_from_response(resp)  # Pass error response to caller
        return resp.json()

    async def send_url(self, urlpath, verb='get', jwt=None, headers={}, **kw):
        """ forward execution request to DApp microservice """
        if jwt:
            headers['Authorization'] = 'Bearer '+jwt
        print(f'*send_url {headers=}, {urlpath=}, {kw=}')
        resp = await self.client.request(verb, urlpath, headers=headers, **kw)
        errors.DAppError.raise_from_response(resp)  # Pass error response to caller
        return resp.json()


class DAppManagerClass(DDHbaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById: dict[principals.DAppId, DAppProxy] = {}  # registry of DApps

    async def register(self, request, session, running_dapp: dapp_attrs.RunningDApp):
        await asyncio.sleep(1)
        from . import pillars  # pillars use DAppManager
        client = httpx.AsyncClient(base_url=running_dapp.location)
        j = await client.get('/app_info')  # get dict of dapp_attrs, one microservice may return multiple DApps
        j.raise_for_status()
        dattrs = j.json()
        for id, attrs in dattrs.items():
            attrs = dapp_attrs.DApp(**attrs)
            proxy = DAppProxy(id=id, running=running_dapp, attrs=attrs, client=client)
            await proxy.initialize(session, pillars.Pillars)  # initialize gets schema and registers everything
            self.DAppsById[typing.cast(principals.DAppId, id)] = proxy
        return

    def bootstrap(self, pillars: dict):
        # schemas.SchemaNetwork.plot(layout='shell_layout')
        return


DAppManager = DAppManagerClass()


class DAppNode(nodes.ExecutableNode):
    """ node managed by a DApp """
    dapp: DAppProxy

    def __hash__(self):
        return hash(self.dapp)

    def __eq__(self, other):
        return (self.dapp == other.dapp) if isinstance(other, DAppNode) else False

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        r = await self.dapp.execute(req)
        return r


class DAppResource(transactions.Resource):

    dapp: DAppProxy | None

    @property
    def id(self) -> str:
        """ key of resource, to be stored in transaction """
        assert self.dapp.attrs.id
        return self.dapp.attrs.id

    @classmethod
    def create(cls, id):
        """ create DAppResource from Id """
        dapp = DAppManager.DAppsById.get(id)
        if not dapp:
            raise errors.NotSelectable(f'Resource DApp {id} not available')
        assert dapp
        return cls(dapp=dapp)

    async def added(self, trx: transactions.Transaction):
        """ Issue begin transaction req to DApp """
        print(f'*DAppResource added {trx=}, {trx.user_token=}')
        await self.begin(trx)
        return

    async def begin(self, trx: transactions.Transaction):
        await self.dapp.send_url(f'transaction/{trx.trxid}/begin', verb='post', jwt=trx.user_token)
        return

    async def commit(self, trx: transactions.Transaction):
        await self.dapp.send_url(f'transaction/{trx.trxid}/commit', verb='post', jwt=trx.user_token)
        return

    async def abort(self, trx: transactions.Transaction):
        await self.dapp.send_url(f'transaction/{trx.trxid}/abort', verb='post', jwt=trx.user_token)
        return
