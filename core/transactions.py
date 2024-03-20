""" transaction API
"""

from __future__ import annotations
import typing
import time
import datetime
import pydantic
import asyncio

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel, CV

from core import permissions, errors, principals, common_ids

import secrets


class TrxAccessError(errors.AccessError): ...
class SessionReinitRequired(TrxAccessError): ...
class TrxOpenError(errors.DDHerror): ...


class TrxExtension(DDHbaseModel):
    """ Plugable Trx extension.
        A subclass of TrxExtension registers itself. It can carry any data and methods,
        and is carried from previous transactions, but .reinit() is called in this case.
    """
    _trx: Transaction | None = None  # back point to parent trx

    def __init_subclass__(cls):
        """ Register this class as extensions"""
        Transaction.TrxExtensions.append(cls)
        cls.class_init(Transaction)

    @classmethod
    def class_init(cls, trx_class: type[Transaction]):
        """ Can be overwritten to perform an action once class has been initialized """
        pass

    def reinit(self):
        """ Can be used to modify TrxExt when passed from a previous Trx """
        pass


class Transaction(DDHbaseModel):
    TrxExtensions: CV[list[type[TrxExtension]]] = []

    trxid: common_ids.TrxId
    owner: principals.Principal
    user_token: str | None = None
    accesses: list[permissions.Access] = pydantic.Field(default_factory=list)
    exp: datetime.datetime = datetime.datetime.now()
    trx_ext: dict[str, TrxExtension] = {}

    actions: list[Action] = pydantic.Field(
        default_factory=list, description="list of actions to be performed at commit")
    resources: dict[str, Resource] = pydantic.Field(
        default_factory=dict, description="dict of resources coordinated")

    trx_local: dict = pydantic.Field(default_factory=dict, description="dict for storage local to transactionn")

    Transactions: typing.ClassVar[dict[common_ids.TrxId, Transaction]] = {}
    TTL: typing.ClassVar[datetime.timedelta] = datetime.timedelta(
        seconds=120)  # max duration of a transaction in seconds (high for debugging)

    @classmethod
    def create(cls, owner: principals.Principal, user_token: str | None = None, **kw) -> Transaction:
        """ Create Trx, and begin it """
        trxid = secrets.token_urlsafe()
        if trxid in cls.Transactions:
            raise KeyError(f'duplicate key: {trxid}')
        trx = cls(trxid=trxid, owner=owner, user_token=user_token, **kw)
        trx.init_extensions()
        trx.begin()
        return trx

    def init_extensions(self):
        """ ensure all TrxExtensions are initialized and present in .trx_ext """
        for te_cls in self.TrxExtensions:
            n = te_cls.__name__
            ext = self.trx_ext.get(n)
            if ext:
                ext._trx = self  # correct backpointer
                ext.reinit()
            else:
                self.trx_ext[n] = ext = te_cls()  # create instance
                ext._trx = self

        return

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
        for resource in self.resources.values():
            await resource.commit(self)
        self.resources.clear()
        return

    async def abort(self):
        for action in self.actions:
            await action.rollback(self)
        self.actions.clear()
        for resource in self.resources.values():
            await resource.rollback(self)
        self.resources.clear()
        self.end()

    def __del__(self):
        """ Async close if transaction is destroyed """
        # print(f'__del__ {self!s}')
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.abort())
            else:
                loop.run_until_complete(self.abort())
        except Exception:
            pass

    def __str__(self):
        """ short summary of transaction """
        return f"Transaction trxid={self.trxid}, #actions={len(self.actions)}, #resources={len(self.resources)}"

    async def __aenter__(self):
        """ use as async context - note that there is currently no awaitable resource, but 
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
        if access.principal:
            if access.principal != self.owner:
                raise TrxAccessError(
                    f'Access.principal {access.principal.id} must correspond to transaction.owner {self.owner.id}')
        else:
            access.principal = self.owner
        self.accesses.append(access)
        return

    def add(self, action: Action):
        """ Add action to this transaction """
        if action.add_ok(self):
            self.actions.append(action)
            action.added(self)
        else:
            raise TrxAccessError(f'action {action} cannot be added to {self}')

    async def add_resource(self, resource: Resource):
        """ Add action to this transaction """
        if resource.add_ok(self):
            self.resources[resource.id] = resource
            await resource.added(self)
        else:
            raise TrxAccessError(f'action {resource} cannot be added to {self}')

    @classmethod
    def get_or_create_transaction_with_id(cls, trxid: common_ids.TrxId, owner: principals.Principal) -> Transaction:
        """ If you need a cross-process trx with a trxid, use this method. """
        trx = cls.Transactions.get(trxid)
        if trx:
            if trx.owner != owner:
                raise TrxOpenError(f'Transaction owner {trx.owner.id} is now requested owner {owner.id}')
            trx.use()
        else:
            trx = cls(trxid=trxid, owner=owner)
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


class Resource(Action):
    """ Remote resource """
    model_config = pydantic.ConfigDict(extra='allow')

    # id: str
    actions: list[Action] = pydantic.Field(
        default_factory=list, description="list of actions to be performed at commit")

    async def added(self, trx: Transaction):
        """ Callback after resource is added """
        return


Transaction.model_rebuild()
