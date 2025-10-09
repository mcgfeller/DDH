""" Session of a DApp against the dapp_api. 
    The goal would be a unique and secure identification of the client's container process,
    but I currently don't know how to do this. Could I tap into a TLS identifier?
"""

import typing
import pydantic
from utils.pydantic_utils import DDHbaseModel


from core import errors, transactions, principals, common_ids, users


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

    def get_transaction(self, create=False) -> transactions.Transaction | None:
        """ get existing trx or create new one
        """
        trx = self.current_trx
        if trx:
            return trx.use()
        elif create:
            return self.create_transaction()
        else:
            return None

    def get_or_create_transaction(self,) -> transactions.Transaction:
        """ always returns transaction, for easier type checking """
        trx = self.get_transaction(create=True)
        assert trx
        return trx

    def create_transaction(self, initial_trx_ext: dict = {}) -> transactions.Transaction:
        """ create a new transaction. raises transactions.TrxOpenError if transaction exists (abort would make the whole thing async).
            TrxExtension dict can be passed from previous Trx. 
        """
        if self.current_trx:
            raise transactions.TrxOpenError('transaction exists, use session.ensure_new_transaction()')
        else:
            new_trx = transactions.Transaction.create(
                owner=self.user, user_token=self.token_str, trx_ext=initial_trx_ext)
            self.current_trx = new_trx
            return new_trx.use()

    async def ensure_new_transaction(self) -> transactions.Transaction:
        """ abort any existing and create a new transaction. 
            Passes TrxExtension dict from previous Trx.
            must be awaited, since trx.abort() is async.
        """
        prev_trx = self.current_trx
        if prev_trx:
            # previous transaction in session
            prev_trx_ext = prev_trx.trx_ext
            await prev_trx.abort()
            self.current_trx = None
        else:
            prev_trx_ext = {}
        return self.create_transaction(initial_trx_ext=prev_trx_ext)

    async def reinit(self):
        prev_trx = self.current_trx  # self.trxs_for_user.get(self.user)
        if prev_trx:
            # abort and clean previous transaction in session
            await prev_trx.abort()
            prev_trx.trx_ext = {}
            self.current_trx = None
        return


def get_system_session() -> Session:
    """ get a session for system purposes """
    return Session(token_str='system_session', user=users.SystemUser)
