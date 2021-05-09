""" Session of a DApp against the dapp_api. 
    The goal would be a unique and secure identification of the client's container process,
    but I currently don't know how to do this. Could I tap into a TLS identifier?
"""
from __future__ import annotations
import typing
import pydantic


from core import permissions,errors


SessionId = typing.NewType('SessionId', str) # identifies the session

class Session(pydantic.BaseModel):
    """ The session is currently identified by its JWT token """
    token_str : str
    user: permissions.User
    dappid: typing.Optional[permissions.DAppId] = None

    @property
    def id(self) -> SessionId:
        """ return id """
        return typing.cast(SessionId,self.token_str)