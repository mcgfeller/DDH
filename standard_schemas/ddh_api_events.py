""" Schemas //org/ddh/events, providing an API against principaled information about DDH itself: Events
"""


import datetime
import typing

import pydantic

from backend import queues
from core import (common_ids, dapp_attrs, errors, events, executable_nodes,
                  keydirectory, keys, nodes, permissions, principals, schemas,
                  trait)
from frontend import sessions
from schema_formats import py_schema
from utils import key_utils, utils
from utils.pydantic_utils import CV


class Subscriptions(py_schema.PySchemaElement):
    """ Subscriptions """
    subscriptions: list[events.SubscribableEvent]

    async def register(self, req: dapp_attrs.ExecuteRequest):
        """ register all subscriptions """
        principal = req.access.principal
        # TODO: Clear subscriptions for principal

        for sub in self.subscriptions:
            topic = sub.get_topic(req.transaction)
            if topic:
                print(f'Subscriptions: registering {topic=}')
                await queues.PubSubQueue.subscribe(topic)
        return

    def __contains__(self, ddhkey: keys.DDHkeyGeneric) -> bool:
        """ check if ddhkey is in subscriptions """

        return any(sub.key == ddhkey for sub in self.subscriptions)


class EventSubscription(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest):
        """ put and get subscriptions for Events.  
        """

        op = req.access.ddhkey.split_at(req.key_split)[1]
        principal = req.access.principal
        assert principal
        print(f'{req.access=}, {req.op=}')

        match req.op:
            case nodes.Ops.get:
                return req.data
            case nodes.Ops.put:
                assert isinstance(req.data, Subscriptions)
                await req.data.register(req)
                return req.data
            case _:
                raise errors.MethodNotAllowed()
        return

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.key: Subscriptions.to_schema()}


class EventQueryParams(trait.QueryParams):
    """ TODO:#35: Additional Query Parameters for EventQuery """
    nowait: bool = False


class EventQuery(executable_nodes.InProcessSchemedExecutableNode):

    MaxEvents: CV[int] = 100

    async def execute(self, req: dapp_attrs.ExecuteRequest) -> list[events.SubscribableEvent]:
        """ obtain given and received consents and return them as a Grants object, which combines
            the key and its consents. 
        """

        op = req.access.ddhkey.split_at(req.key_split)[1]
        principal = req.access.principal
        wait_on_key = op.ensure_rooted()
        print(f'{req.access=}, {req.op=}, {req.query_params=}')
        assert principal
        # we need to retrieve the principal's subscription:
        subscriptions = await self.get_subscriptions(req)
        if not subscriptions:
            raise errors.NotFound(f'No subscription for {wait_on_key=}')

        # check if user is subscribed to this key:
        if not wait_on_key in subscriptions:
            raise errors.NotFound(f'No subscription for {wait_on_key=}')

        topic = events.UpdateEvent.keyy2topic(wait_on_key, req.transaction)
        evs = []
        if topic:
            print(f'EventQuery: waiting on {topic=}')
            async for jev in await queues.PubSubQueue.listen_upto(topic, many=self.MaxEvents):
                ev = events.SubscribableEvent.create_from_json(jev)
                if await self.check_access(ev, req):
                    evs.append(ev)
        else:
            raise errors.NotFound(f'No topic for {wait_on_key=}')

        return evs

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        schema = Subscriptions.to_schema()  # TOOD:#35 - own schema?
        # register our EventQueryParams:
        schema.schema_attributes.register_query_params(EventQueryParams)
        return {self.key: schema}

    async def get_subscriptions(self, req) -> Subscriptions | None:
        """ return subscriptions for this principal """
        subscriptions = None
        subs_key = keys.DDHkey('//org/ddh/events/subscriptions').with_new_owner(req.access.principal.id)
        subs_node, split = await keydirectory.NodeRegistry.get_node_async(subs_key, nodes.NodeSupports.data, req.transaction)
        if subs_node:
            subs_node = await subs_node.ensure_loaded(req.transaction)
            data = key_utils.nested_get_key(subs_node.data, subs_key.key[2:])
            if data:
                subscriptions = Subscriptions.model_validate(data)

        return subscriptions

    async def check_access(self, ev, req) -> bool:
        access = permissions.Access(ddhkey=ev.key, principal=req.access.principal)
        # we need to get the consent node:
        try:
            consent_node, c_key_split = await keydirectory.NodeRegistry.get_node_async(ev.key, nodes.NodeSupports.consents, req.transaction)

        except errors.AccessError:
            # we have no access to the consent node; decide without the node (no access decision)
            consent_node = None
        ok, *dummy = access.permitted(consent_node, owner=None)
        return ok


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    attrs = dapp_attrs.SchemaProvider()
    csub = EventSubscription(owner=principals.RootPrincipal, attrs=attrs,
                             key=keys.DDHkeyVersioned0('//org/ddh/events/subscriptions'))
    csub.register(session)

    attrs = dapp_attrs.SchemaProvider()
    cquery = EventQuery(owner=principals.RootPrincipal, attrs=attrs,
                        key=keys.DDHkeyVersioned0('//org/ddh/events/wait'))
    cquery.register(session)


install()
