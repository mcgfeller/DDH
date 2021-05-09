""" DDH abstract storage
"""
from __future__ import annotations
from abc import abstractmethod
import typing

from core import keys,permissions,nodes
from utils.pydantic_utils import NoCopyBaseModel


class Storage(NoCopyBaseModel):

    byId : dict[nodes.NodeId,nodes.Persistable] = {}

    def store(self,node: nodes.Persistable):
        self.byId[node.id] = node
        return

    def load(self,id : nodes.NodeId) -> typing.Optional[nodes.Persistable]:
        # sourcery skip: inline-immediately-returned-variable
        n = self.byId.get(id,None)
        return n



class OwnedStorage(Storage):
    """ Storage for a particular owners """

    owners : tuple[permissions.Principal]

class OwnerStorage(Storage):

    byOwners : dict[tuple[permissions.Principal,...],OwnedStorage] = {}

    def store(self,node: nodes.Node):
        o = node.owners
        owned_storage = self.byOwners.get(o,OwnedStorage(owners=o))
        return owned_storage.store(node)

    def load(self,id : nodes.NodeId) -> typing.Optional[nodes.Node]:
        o = node.owners
        owned_storage = self.byOwners.get(o,OwnedStorage(owners=o))
        n = owned_storage.load(id)
        return n

