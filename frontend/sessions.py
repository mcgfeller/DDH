""" Session of a DApp against the dapp_api. 
    The goal would be a unique and secure identification of the client's container process,
    but I currently don't know how to do this. Could I tap into a TLS identifier?
"""
from __future__ import annotations
import typing
import pydantic
from utils.pydantic_utils import NoCopyBaseModel


from core import permissions,errors,transactions


SessionId = typing.NewType('SessionId', str) # identifies the session


class Session(NoCopyBaseModel):
    """ The session is currently identified by its JWT token """
    token_str : str
    user: permissions.User
    dappid: typing.Optional[permissions.DAppId] = None
    trxs_for_user: dict[permissions.Principal,transactions.Transaction] = pydantic.Field(default_factory=dict)

    @property
    def id(self) -> SessionId:
        """ return id """
        return typing.cast(SessionId,self.token_str)

    def get_transaction(self,for_user : permissions.Principal,create=False) -> typing.Optional[transactions.Transaction]:
        """ get existing trx or create new one """
        trx = self.trxs_for_user.get(for_user)
        if trx:
            return trx.use() 
        elif create:
            return self.new_transaction(for_user=for_user)
        else:
            return None

    def new_transaction(self,for_user : permissions.Principal) -> transactions.Transaction:
        prev_trx = self.trxs_for_user.get(for_user)
        if prev_trx:
            # previous transaction in session
            prev_trx.abort()
        new_trx =  transactions.Transaction.create(for_user = for_user)
        return new_trx.use()

