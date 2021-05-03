""" transaction API
"""

from __future__ import annotations
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from core import pillars
from core import keys,permissions,schemas,nodes,errors

import secrets

class TrxAccessError(errors.AccessError): ...

TrxId = typing.NewType('TrxId',str)

class Transaction(NoCopyBaseModel):
    trxid : TrxId 

    read_owners : set[permissions.Principal] = set()
    read4write_consented : set[permissions.Principal] =  {permissions.AllPrincipal}

    Transactions : typing.ClassVar[dict] = {}

    @classmethod
    def create(cls,session) -> Transaction:
        trxid = secrets.token_urlsafe()
        if trxid in cls.Transactions:
            raise KeyError(f'duplicate key: {trxid}')
        trx = cls(trxid=trxid)
        cls.Transactions[trx.trxid] = trx
        return trx

    @classmethod
    def get(cls,trxid : TrxId) -> Transaction:
        return cls.Transactions[trxid]

    @staticmethod
    def _get_owners(nodes: typing.Iterable[nodes.Node]) -> set[permissions.Principal]:
        """ set of all owners of nodes """
        return set.union(*[set(node.owners) for node in nodes])

    @staticmethod
    def _get_consents(nodes: typing.Iterable[nodes.Node]) -> dict[permissions.Principal, permissions.Consents]:
        """ get the consent of all nodes by owner """
        return {}    

    @staticmethod
    def _get_consent_owners(nodes: typing.Iterable[nodes.Node], access_mode: permissions.AccessMode) -> set[permissions.Principal]:
        """ get a set of principals for which we have access_mode permission in nodes """
        return {permissions.AllPrincipal}

    def read(self, nodes: typing.Iterable[nodes.Node]):
        self.read_owners |= self._get_owners(nodes)

    def read_for_write(self, nodes: typing.Iterable[nodes.Node]):
        self.read4write_consented &= self._get_consent_owners(nodes,permissions.AccessMode.protected)


    def write(self, nodes: typing.Iterable[nodes.Node]):
        # all reads must be read4write
        not4write = self.read_owners - self.read4write_consented
        if not4write:
            raise TrxAccessError(f"Cannot write: reads requested, but not read_for_write for owners {', '.join(map(str,not4write))}")
        not_consented = self._get_owners(nodes) - self.read4write_consented
        if not_consented:
            raise TrxAccessError(f"Cannot write to nodes, because we've read notes that do not consent to write to owners {', '.join(map(str,not_consented))}")        
        return

    def end(self):
        self.Transactions.pop(self.trxid)