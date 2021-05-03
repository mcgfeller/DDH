from __future__ import annotations
import typing
import pydantic


from core import permissions,errors


SessionId = typing.NewType('SessionId', str)

class Session(pydantic.BaseModel):
    token_str : str
    user: permissions.User

    @property
    def key(self) -> SessionId:
        return typing.cast(SessionId,self.token_str)