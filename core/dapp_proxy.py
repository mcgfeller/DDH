""" Proxy representing DApps in runner  """
from __future__ import annotations
import enum
import typing
import pydantic
import pydantic.json
import json
import asyncio

import logging
import pprint

logger = logging.getLogger(__name__)

# from frontend import sessions
from core import keys, schemas as m_schemas, nodes, keydirectory, errors, transactions, principals, relationships, schema_network, dapp_attrs
from utils.pydantic_utils import DDHbaseModel


class DAppProxy(DDHbaseModel):
    """ This is a Proxy running in the Walled Garden of DApps (running in their own microservices) """

    id: str
    running:  dapp_attrs.RunningDApp
    attrs: dapp_attrs.DApp

    schemas: dict[keys.DDHkeyVersioned, m_schemas.AbstractSchema] = {}

    async def initialize_schemas(self, session, pillars: dict):
        if True:  # self.running.schema_version > versions._UnspecifiedVersion:
            j = await (self.running.client.get('/schemas'))
            j.raise_for_status()
            js = j.json()
            # print(f'***schemas')
            # pprint.pprint(js)
            self.schemas = {keys.DDHkeyVersioned(k): m_schemas.AbstractSchema.create_schema(s, sf, sa)
                            for k, (sa, sf, s) in js.items()}
            self.register_schemas(session)

        return

    def register_schemas(self, session) -> list[nodes.Node]:
        """ We register: 
            - SchemaNode for the Schemas our node provides, including transformed-into keys.

        """
        transaction = session.get_or_create_transaction()

        snodes = []
        for schemakey, schema in self.schemas.items():
            # print(f'*register_schemas {schemakey=}, {type(schema)=}')
            snode = self.register_schema(schemakey, schema, self.attrs.owner, transaction)

            #
            snodes.append(snode)
        return snodes

    @staticmethod
    def register_schema(schemakey: keys.DDHkeyVersioned, schema: m_schemas.AbstractSchema, owner: principals.Principal, transaction):
        """ register a single schema in its Schema Node, creating one if necessary. 
            staticmethod so it can be called by test fixtures. 
        """
        genkey = schemakey.without_variant_version()
        snode = keydirectory.NodeRegistry[genkey].get(
            nodes.NodeSupports.schema)  # need exact location, not up the tree
        # hook into parent schema:
        parent, split = m_schemas.AbstractSchema.get_parent_schema(transaction, genkey)
        # inherit transformers:
        schema.schema_attributes.transformers = parent.schema_attributes.transformers.merge(
            schema.schema_attributes.transformers)

        if snode:
            # TODO:#33: Schema Nodes don't need .ensure_loaded now, but should be reinserted once they're async
            snode = typing.cast(nodes.SchemaNode, snode)
            snode.add_schema(schema)
        else:
            # create snode with our schema:
            snode = nodes.SchemaNode(owner=owner, consents=m_schemas.AbstractSchema.get_schema_consents())
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
        dnode = DAppNode(owner=attrs.owner, dapp=self, consents=m_schemas.AbstractSchema.get_schema_consents())
        schema_network.add_dapp(attrs)

        for ref in attrs.references:
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
        resp = await self.running.client.post('execute', data=req.model_dump_json())
        errors.DAppError.raise_from_response(resp)  # Pass error response to caller
        return resp.json()

    async def send_url(self, urlpath, verb='get', jwt=None, headers={}, **kw):
        """ forward execution request to DApp microservice """
        if jwt:
            headers['Authorization'] = 'Bearer '+jwt
        # print(f'*send_url {headers=}, {urlpath=}, {kw=}')
        resp = await self.running.client.request(verb, urlpath, headers=headers, **kw)
        errors.DAppError.raise_from_response(resp)  # Pass error response to caller
        return resp.json()


class DAppManagerClass(DDHbaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById: dict[principals.DAppId, DAppProxy] = {}  # registry of DApps

    async def register(self, request, session, running_dapp: dapp_attrs.RunningDApp):
        from . import pillars  # pillars use DAppManager
        # get dict of dapp_attrs, one microservice may return multiple DApps
        j = await running_dapp.client.get('/app_info')
        j.raise_for_status()
        dattrs = j.json()
        assert len(dattrs) > 0
        for id, attrs in dattrs.items():  # register individual apps and references.
            attrs = dapp_attrs.DApp(**attrs)
            proxy = DAppProxy(id=id, running=running_dapp, attrs=attrs)
            proxy.register_references(session, m_schemas.SchemaNetwork)
            self.DAppsById[typing.cast(principals.DAppId, id)] = proxy
        await proxy.initialize_schemas(session, pillars.Pillars)  # get schemas and register them
        m_schemas.SchemaNetwork.valid.invalidate()  # finished
        return

    def bootstrap(self, pillars: dict):
        # m_schemas.SchemaNetwork.plot(layout='shell_layout')
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
