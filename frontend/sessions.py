""" Session of a DApp against the dapp_api. 
    The goal would be a unique and secure identification of the client's container process,
    but I currently don't know how to do this. Could I tap into a TLS identifier?
"""
from __future__ import annotations
import typing
import pydantic
from utils.pydantic_utils import DDHbaseModel


from core import errors, transactions, principals, common_ids, users


# TODO: Should we have a single current trx per session or something per user?

class Session(DDHbaseModel):
    """ The session is currently identified by its JWT token """
    token_str: str
    user: users.User
    dappid: principals.DAppId | None = None
    trxs_for_user: dict[principals.Principal, transactions.Transaction] = pydantic.Field(default_factory=dict)
    current_trx: transactions.Transaction | None = None

    @property
    def id(self) -> common_ids.SessionId:
        """ return id """
        return typing.cast(common_ids.SessionId, self.token_str)

    def get_transaction(self, for_user: principals.Principal | None = None, create=False) -> transactions.Transaction | None:
        """ get existing trx or create new one
            for_user defaults to session.user
        """
        for_user = for_user or self.user
        trx = self.current_trx  # trxs_for_user.get(for_user)
        if trx:
            return trx.use()
        elif create:
            return self.create_transaction(for_user=for_user)
        else:
            return None

    def get_or_create_transaction(self, for_user: principals.Principal | None = None) -> transactions.Transaction:
        """ always returns transaction, for easier type checking """
        trx = self.get_transaction(for_user=for_user, create=True)
        assert trx
        return trx

    def create_transaction(self, for_user: principals.Principal | None = None, initial_read_consentees=transactions.DefaultReadConsentees) -> transactions.Transaction:
        """ create a new transaction. raises transactions.TrxOpenError if transaction exists (abort would make the whole thing async). """
        if self.current_trx:
            raise transactions.TrxOpenError('transaction exists, use session.ensure_ensure_new_transaction()')
        else:
            for_user = for_user or self.user
            new_trx = transactions.Transaction.create(
                for_user=for_user, initial_read_consentees=initial_read_consentees)
            self.trxs_for_user[for_user] = new_trx
            self.current_trx = new_trx
            return new_trx.use()

    async def ensure_new_transaction(self, for_user: principals.Principal | None = None) -> transactions.Transaction:
        """ abort any existing and create a new transaction.
            must be awaited, since trx.abort() is async.
        """
        for_user = for_user or self.user
        prev_trx = self.current_trx  # self.trxs_for_user.get(for_user)
        if prev_trx:
            # previous transaction in session
            prev_read_consentees = prev_trx.read_consentees
            await prev_trx.abort()
            self.trxs_for_user.pop(for_user, None)
            self.current_trx = None
        else:
            prev_read_consentees = transactions.DefaultReadConsentees
        return self.create_transaction(for_user, initial_read_consentees=prev_read_consentees)

    async def reinit(self):
        prev_trx = self.current_trx  # self.trxs_for_user.get(self.user)
        if prev_trx:
            # abort and clean previous transaction in session
            await prev_trx.abort()
            prev_trx.read_consentees = transactions.DefaultReadConsentees
            self.trxs_for_user.pop(prev_trx.for_user, None)
            self.current_trx = None
        return


def get_system_session() -> Session:
    """ get a session for system purposes """
    return Session(token_str='system_session', user=users.SystemUser)
