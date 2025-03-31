""" Queue interface:
    Queues are currently mocked.
"""

from __future__ import annotations

import typing
import logging
import pydantic
from utils.pydantic_utils import DDHbaseModel, CV
from core import keys
import asyncio


class Queue(DDHbaseModel):
    """ Primitive cover to asyncio.Queue """
    _queue: asyncio.Queue = asyncio.Queue()
    _AllQueues: CV[list[Queue]] = []  # register all queues, mainly for monitoring and testing

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._AllQueues.append(self)  # register this Queue

    async def put(self, item):
        return await self._queue.put(item)

    async def get(self):
        # print('get queue', hex(id(self._queue)), self._queue)
        x = await self._queue.get()
        # print('got queue', type(x))
        return x

    async def get_upto(self, many=1):
        """ Get up to many entries, waiting for the first, not waiting thereafter """
        yield await self._queue.get()
        for i in range(1, many):
            try:
                yield self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        return

    def qsize(self):
        return self._queue.qsize()


class _PubSubQueue(Queue):
    """ Global Pub/Sub Queue:
        Emulate by creating a Queue for each topic. 
    """
    _subscriptions: dict[Topic, Queue] = {}

    def _get_queue(self, topic: Topic, raise_error=False) -> Queue | None:
        q = self._subscriptions.get(topic, None)
        if raise_error and not q:
            raise ValueError('must subscribe to topic {topic}')
        return q

    async def subscribe(self, topic: Topic) -> Queue:
        q = self._get_queue(topic)
        if not q:
            self._subscriptions[topic] = q = Queue()
        return q

    async def listen(self, topic: Topic):
        return await self._get_queue(topic, raise_error=True).get(topic)

    async def listen_upto(self, topic: Topic,  many=1):
        return self._get_queue(topic, raise_error=True).get_upto(many=many)

    async def publish(self, topic: Topic, event):
        q = self._get_queue(topic)
        if q:
            return await q.put(event)
        else: return None  # do nothing


PubSubQueue = _PubSubQueue()  # the global PUB/SUB Queue


class Topic(str):
    """ Pub/Sub topic """

    @classmethod
    def update_topic(cls, key: keys.DDHkey) -> typing.Self:
        """ creates an update topic for a ressource under key """
        return cls(str(key))


async def wait_for_empty_queues(sleep: float = 0.1, maxwait: float = 2.0):
    """ Wait till all registered queues are empty. Useful for testing. """
    for i in range(int(maxwait//sleep)):
        for q in Queue._AllQueues:
            if q._queue.empty():
                continue
            else:
                await asyncio.sleep(sleep)
    return
