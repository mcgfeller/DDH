""" Events, for which an API to subscribe exits.
"""
from __future__ import annotations

import datetime
import pydantic

from core import keys, keydirectory, nodes
from utils import utils
from backend import queues
from utils.pydantic_utils import DDHbaseModel


class SubscribableEvent(DDHbaseModel):
    """ Event on a single DDHkey. Potential for extension to kind of event and update specifics
    """
    key: keys.DDHkeyGeneric

    def get_topic(self, transaction) -> queues.Topic | None:
        """ get a topic for key.
            Topics key is the next subscribable schema
        """
        s_key = self.key.ens()
        schema, s_split = keydirectory.NodeRegistry.get_node(s_key, nodes.NodeSupports.subscribable, transaction)
        if schema:
            s_key, remainder = s_key.split_at(s_split)
            e_key = s_key.ensure_fork(self.key.fork).without_variant_version()  # publish generic key
            topic = queues.Topic('change_event:'+str(e_key))
        else:
            topic = None
        return topic

    async def publish(self, transaction) -> None:
        """ publish the event to the topic """
        topic = self.get_topic(transaction)
        if topic:
            await queues.PubSubQueue.publish(topic, self.model_dump_json())
        return


class UpdateEvent(SubscribableEvent):
    key: keys.DDHkey

    timestamp: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
