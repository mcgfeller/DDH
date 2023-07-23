""" transaction API
"""

from __future__ import annotations
import typing
import time
import datetime
import pydantic
import asyncio

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from core import permissions, errors, principals, common_ids

import secrets


class TrxAccessError(errors.AccessError): ...
class TrxOpenError(errors.DDHerror): ...


DefaultReadConsentees = {principals.AllPrincipal.id}  # by default, nothing is readable by everybody


class Transaction(DDHbaseModel):
    trxid: common_ids.TrxId
    for_user: principals.Principal
    accesses: list[permissions.Access] = pydantic.Field(default_factory=list)
    exp: datetime.datetime = datetime.datetime.now()

    # with nothing read, the world has access
    read_consentees: set[common_ids.PrincipalId] = DefaultReadConsentees
    # same as read_consentees, but not modified during transaction
    initial_read_consentees:  set[common_ids.PrincipalId] = DefaultReadConsentees

    actions: list[Action] = pydantic.Field(
        default_factory=list, description="list of actions to be performed at commit")
    trx_local: dict = pydantic.Field(default_factory=dict, description="dict for storage local to transactionn")

    # https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    Transactions: typing.ClassVar[dict[common_ids.TrxId, typing.Any]] = {}
    # Transactions : typing.ClassVar[dict[common_ids.TrxId,'Transaction']] = {}
    TTL: typing.ClassVar[datetime.timedelta] = datetime.timedelta(
        seconds=30)  # max duration of a transaction in seconds

    def __init__(self, **kw):
        super().__init__(**kw)
        self.read_consentees = self.initial_read_consentees
        return

    @classmethod
    def create(cls, for_user: principals.Principal, **kw) -> Transaction:
        """ Create Trx, and begin it """
        trxid = secrets.token_urlsafe()
        if trxid in cls.Transactions:
            raise KeyError(f'duplicate key: {trxid}')
        trx = cls(trxid=trxid, for_user=for_user, **kw)
        trx.begin()
        return trx

    @classmethod
    def get(cls, trxid: common_ids.TrxId) -> Transaction:
        return cls.Transactions[trxid].use()

    @classmethod
    def get_or_raise(cls, trxid: common_ids.TrxId, error=errors.NotFound) -> Transaction:
        """ get transaction or raise HTTP error """
        trx = cls.Transactions.get(trxid)
        if trx:
            return trx.use()
        else:
            raise error(trxid).to_http()

    def begin(self):
        """ begin this transaction """
        self.Transactions[self.trxid] = self
        self.exp = datetime.datetime.now() + self.TTL
        return

    def end(self):
        """ end this transaction """
        if self.actions:
            raise TrxOpenError(f'Transaction has {len(self.actions)} pending actions. Either .commit() or .abort() it.')
        self.Transactions.pop(self.trxid, None)
        return

    async def commit(self):
        """ commit a transaction by committing all actions """
        for action in self.actions:
            await action.commit(self)
        self.actions.clear()
        return

    async def abort(self):
        for action in self.actions:
            await action.rollback(self)
        self.actions.clear()
        self.end()

    def __del__(self):
        """ Async close if transaction is destroyed """
        print(f'__del__ {self=}')
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.abort())
            else:
                loop.run_until_complete(self.abort())
        except Exception:
            pass

    async def __aenter__(self):
        """ use as async context - note that there is currently no awaitable ressource, but 
            this might change with storages.  

            Note: There is no sync variant - you have to use an async context. 
        """
        self.begin()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """ Note: There is no sync variant - you have to use an async context.  
        """
        if exc_type is None:
            await self.commit()
        else:
            await self.abort()

    def use(self):
        if datetime.datetime.now() > self.exp:
            raise TrxAccessError(f'Transaction has expired; {self.TTL=}')
        return self

    def add_and_validate(self, access: permissions.Access):
        """ add an access and validate whether it is ok """
        self.accesses.append(access)
        if permissions.AccessMode.write in access.modes:  # we must check writes for presence of read objects
            if principals.AllPrincipal.id not in self.read_consentees and access.ddhkey.owner not in self.read_consentees:
                msg = f'transactions contains data with no consent to use for {access.ddhkey.owner}'
                if principals.AllPrincipal.id not in self.initial_read_consentees and access.ddhkey.owner not in self.initial_read_consentees:
                    # this transaction contains data from previous transaction, must reinit
                    raise TrxAccessError('call session.reinit(); '+msg)
                else:
                    raise TrxAccessError(msg)
        return

    def add_read_consentees(self, read_consentees: set[common_ids.PrincipalId]):
        if principals.AllPrincipal.id in self.read_consentees:
            self.read_consentees = read_consentees
        else:
            self.read_consentees &= read_consentees
        return

    def add(self, action: Action):
        """ Add action to this transaction """
        if action.add_ok(self):
            self.actions.append(action)
            action.added(self)
        else:
            raise TrxAccessError(f'action {action} cannot be added to {self}')

    @classmethod
    def get_or_create_transaction_with_id(cls, trxid: common_ids.TrxId, for_user: principals.Principal) -> Transaction:
        """ If you need a cross-process trx with a trxid, use this method. """
        trx = cls.Transactions.get(trxid)
        if trx:
            trx.use()
        else:
            trx = cls(trxid=trxid, for_user=for_user)
            trx.begin()
        return trx


class Action(DDHbaseModel):
    """ actions for a transaction """

    def added(self, trx: Transaction):
        """ Callback after transaction is added """
        return

    def add_ok(self, trx: Transaction) -> bool:
        """ Callback to determine whether it is ok to add action to trx """
        return True

    async def commit(self, transaction):
        """ commit an action, called by transaction.commit() """

        return

    async def rollback(self, transaction):
        """ rollback an action, called by transaction.rollback() """

        return


Transaction.update_forward_refs()
