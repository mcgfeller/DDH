""" Events, for which an API to subscribe exits.
"""


import datetime
import typing
import pydantic
import pydantic_core

from core import keys, keydirectory, nodes, common_ids
from utils import utils
from backend import queues
from utils.pydantic_utils import DDHbaseModel, CV


class SubscribableEvent(DDHbaseModel):
    """ Event on a single DDHkey. Potential for extension to kind of event and update specifics. 
        Records subclass in json, so can be recreated from json. 
    """
    key: keys.DDHkeyGeneric
    topic_prefix: CV[str] = 'update'
    _EventClasses: CV[dict[str, type[SubscribableEvent]]] = {}
    event_class: str | None = None

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        SubscribableEvent._EventClasses[cls.__name__] = cls

    @classmethod
    def get_class_name(cls) -> str:
        return cls.__name__

    def __init__(self, *a, **kw):
        """ Ensure .event_class records name of concrete class, so we can JSON object and back. """
        if 'event_class' not in kw:
            kw['event_class'] = self.__class__.__name__
        super().__init__(*a, **kw)

    @classmethod
    def create_from_json(cls, j: str) -> SubscribableEvent:
        """ Recreate JSON'ed object in correct class, based on .event_class attribute """
        ev = pydantic_core.from_json(j)  # get dict from string
        cls = cls._EventClasses.get(ev['event_class'], cls)
        return cls.model_validate(ev)

    def get_topic(self, transaction) -> queues.Topic | None:
        """ get a topic for key.
            Topic key is the next subscribable schema
        """
        return self.keyy2topic(self.key, transaction)

    @classmethod
    def keyy2topic(cls, key: keys.DDHkeyGeneric, transaction) -> queues.Topic | None:
        """ get a topic for key.
            Topic key is the next subscribable schema

            TODO #35: This needs a cache
        """
        s_key = key.ens()
        schema, s_split = keydirectory.NodeRegistry.get_node(s_key, nodes.NodeSupports.subscribable, transaction)
        if schema:
            s_key, remainder = s_key.split_at(s_split)
            e_key = s_key.ensure_fork(key.fork).without_variant_version()  # publish generic key
            topic = queues.Topic(cls.topic_prefix+':'+str(e_key))
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


class ConsentEvent(UpdateEvent):
    grants_added: set[keys.DDHkeyGeneric]

    @classmethod
    def for_principal(cls, principal: common_ids.PrincipalId, grants_added: set[keys.DDHkeyGeneric]):
        key = keys.DDHkeyGeneric('//org/ddh/consents/received/').with_new_owner(principal)
        return cls(key=key, grants_added=grants_added)
