""" transaction API
"""

from __future__ import annotations
import typing
import time
import datetime
import pydantic

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from core import permissions,nodes,errors

import secrets


class TrxAccessError(errors.AccessError): ...

TrxId = typing.NewType('TrxId',str)


class Transaction(NoCopyBaseModel):
    trxid : TrxId 
    for_user: permissions.Principal
    accesses: list[permissions.Access] = pydantic.Field(default_factory=list)
    exp : datetime.datetime = datetime.datetime.now()

    read_consentees : typing.Optional[set[permissions.Principal]] = None # don't initialize with empty set - first assignment is set
    initial_read_consentees :  typing.Optional[set[permissions.Principal]] = None # same as read_consentees, but not modified during transaction

    Transactions : typing.ClassVar[dict[TrxId,'Transaction']] = {}
    TTL : typing.ClassVar[datetime.timedelta] = datetime.timedelta(seconds=5) # max duration of a transaction in seconds

    def __init__(self,**kw):
        super().__init__(**kw)
        self.read_consentees = self.initial_read_consentees
        return


    @classmethod
    def create(cls,for_user : permissions.Principal,initial_read_consentees : typing.Optional[set[permissions.Principal]] = None) -> Transaction:
        """ Create Trx, and begin it """
        trxid = secrets.token_urlsafe()
        if trxid in cls.Transactions:
            raise KeyError(f'duplicate key: {trxid}')
        trx = cls(trxid=trxid,for_user=for_user)
        trx.begin()
        return trx

    @classmethod
    def get(cls,trxid : TrxId) -> Transaction:
        return cls.Transactions[trxid].use()


    def begin(self):
        """ begin this transaction """
        self.Transactions[self.trxid] = self
        self.exp = datetime.datetime.now() + self.TTL
        return

    def end(self):
        """ end this transaction """
        self.Transactions.pop(self.trxid,None)
        return

    def abort(self):
        self.end()

    def __del__(self):
        self.abort()
    
    def use(self):
        if datetime.datetime.now() > self.exp:
            raise TrxAccessError(f'Transaction has expired; {self.TTL=}')
        return self


    def write(self, nodes: typing.Iterable[nodes.Node]):
        # all reads must be read4write
        not4write = self.read_owners - self.read4write_consented
        if not4write:
            raise TrxAccessError(f"Cannot write: reads requested, but not read_for_write for owners {', '.join(map(str,not4write))}")
        not_consented = self._get_owners(nodes) - self.read4write_consented
        if not_consented:
            raise TrxAccessError(f"Cannot write to nodes, because we've read notes that do not consent to write to owners {', '.join(map(str,not_consented))}")        
        return

    def add_and_validate(self, access : permissions.Access):
        """ add an access and validate whether it is ok """
        self.accesses.append(access)
        # TODO: Validation: 
        # TODO: Add read,read,write test
        # TODO: Handle world access
        if self.read_consentees is not None and  access.ddhkey.owners not in self.read_consentees:
            if self.initial_read_consentees is not None and access.ddhkey.owners not in self.initial_read_consentees:
                # this transaction contains data from previous transaction, must reinit
                raise TrxAccessError('must reinit')
            else:
                raise TrxAccessError(f'transactions contains data with no consent to use for {access.ddhkey.owners}')
        return

    def add_read_consentees(self, read_consentees: set[permissions.Principal]):
        if self.read_consentees is None:
            self.read_consentees = read_consentees
        else:
            self.read_consentees &= read_consentees
        return

