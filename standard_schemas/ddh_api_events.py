""" Schemas //org/ddh/events, providing an API against principaled information about DDH itself: Events
"""
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, executable_nodes, principals, errors, permissions, common_ids, dapp_attrs, nodes, events
from utils import utils
from frontend import sessions
from backend import queues
from schema_formats import py_schema


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


class EventQuery(executable_nodes.InProcessSchemedExecutableNode):

    async def execute(self, req: dapp_attrs.ExecuteRequest) -> list[events.SubscribableEvent]:
        """ obtain given and received consents and return them as a Grants object, which combines
            the key and its consents. 
        """

        op = req.access.ddhkey.split_at(req.key_split)[1]
        principal = req.access.principal
        wait_on_key = op.ensure_rooted()
        assert principal
        print(f'{req.access=}, {req.op=}')
        topic = queues.Topic.update_topic(wait_on_key)
        r = await queues.PubSubQueue.listen(topic)
        return r

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.key: Subscriptions.to_schema()}  # TOOD:#35


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
