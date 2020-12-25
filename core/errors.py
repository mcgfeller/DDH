""" DDH Core Custom Errors """
from __future__ import annotations
import typing
import fastapi

class DDHerror(RuntimeError):
    http_status : typing.ClassVar[int] = 500

    def to_http(self):
        return fastapi.HTTPException(status_code=self.http_status, detail=str(self))

class AccessError(DDHerror):
    http_status = 403

class NotFound(DDHerror):
    http_status = 404

class DAppError(DDHerror): pass


SubClass = NotImplementedError('must be implemented in subclass')