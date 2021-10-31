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
DefaultReadConsentees = {permissions.AllPrincipal.id} # by default, nothing is readable by everybody

class Transaction(NoCopyBaseModel):
    trxid : TrxId 
    for_user: permissions.Principal
    accesses: list[permissions.Access] = pydantic.Field(default_factory=list)
    exp : datetime.datetime = datetime.datetime.now()

    read_consentees : set[permissions.PrincipalId] = DefaultReadConsentees # with nothing read, the world has access
    initial_read_consentees :  set[permissions.PrincipalId] = DefaultReadConsentees # same as read_consentees, but not modified during transaction

    Transactions : typing.ClassVar[dict[TrxId,'Transaction']] = {}
    TTL : typing.ClassVar[datetime.timedelta] = datetime.timedelta(seconds=30) # max duration of a transaction in seconds

    def __init__(self,**kw):
        super().__init__(**kw)
        self.read_consentees = self.initial_read_consentees
        return


    @classmethod
    def create(cls,for_user : permissions.Principal,**kw) -> Transaction:
        """ Create Trx, and begin it """
        trxid = secrets.token_urlsafe()
        if trxid in cls.Transactions:
            raise KeyError(f'duplicate key: {trxid}')
        trx = cls(trxid=trxid,for_user=for_user,**kw)
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


    def add_and_validate(self, access : permissions.Access):
        """ add an access and validate whether it is ok """
        self.accesses.append(access)
        if permissions.AccessMode.write in access.modes: # we must check writes for presence of read objects
            if permissions.AllPrincipal.id not in self.read_consentees and  access.ddhkey.owners not in self.read_consentees:
                msg = f'transactions contains data with no consent to use for {access.ddhkey.owners}'
                if permissions.AllPrincipal.id not in self.initial_read_consentees and access.ddhkey.owners not in self.initial_read_consentees:
                    # this transaction contains data from previous transaction, must reinit
                    raise TrxAccessError('call session.reinit(); '+msg)
                else:
                    raise TrxAccessError(msg)
        return

    def add_read_consentees(self, read_consentees: set[permissions.PrincipalId]):
        if permissions.AllPrincipal.id in self.read_consentees:
            self.read_consentees = read_consentees
        else:
            self.read_consentees &= read_consentees
        return

