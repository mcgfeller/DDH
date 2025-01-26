""" DDH Core Custom Errors """
from __future__ import annotations
import typing
import fastapi


class DDHerror(RuntimeError):
    http_status: typing.ClassVar[int] = 500
    by_http_status: typing.ClassVar[dict[int, type[DDHerror]]] = {}

    def to_http(self):
        return fastapi.HTTPException(status_code=self.http_status, detail=str(self))

    def __init_subclass__(cls, **kwargs):
        """ collect errors by status """
        super().__init_subclass__(**kwargs)
        if cls.http_status != 500:
            cls.by_http_status[cls.http_status] = cls
        return

    @classmethod
    def raise_from_response(cls, response):
        """ raise error from http response, recreating specific DDHerror """
        if not response.is_success:
            error_class = cls.by_http_status.get(response.status_code)
            if error_class:
                try:
                    detail = response.json().get('detail')
                except:
                    detail = response.text
                err = error_class(detail)  # make error
                raise err.to_http()  # ...and raise it
            else:
                response.raise_for_status()  # no error we know - raise default
        return  # all good!


class AccessError(DDHerror):
    http_status = 403


class DecryptionError(AccessError):
    """ Errors encountered during accessing encrypted data """
    ...


class DAppAuthorizationError(DDHerror):
    """ Delegated request does not access this user """
    http_status = 401


class CapabilityMissing(AccessError):
    """ The required capability is missing to fullfill this request """


class NotFound(DDHerror):
    http_status = 404


class MethodNotAllowed(DDHerror):
    """ This key does not support this method. """
    http_status = 405


class NotSelectable(DDHerror):
    """ This key cannot be selected because resource has no supported substructure. """
    http_status = 406


class NotAcceptable(DDHerror):
    """ This schema does not correspond to the Accept header media types. """
    http_status = 406


class ValidationError(DDHerror):
    """ The data cannot be validated against the schema. """
    http_status = 422


class ParseError(ValidationError):
    """ The data cannot be parsed """
    ...


class VersionMismatch(ValidationError):
    """ Schema and data versions do not match """
    ...


class DAppError(DDHerror): pass


SubClass = NotImplementedError('must be implemented in subclass')
