""" Record Audit events, with AuditPersistAction """


from __future__ import annotations
import typing


from utils.pydantic_utils import DDHbaseModel
from core import permissions
from . import persistable


class AuditRecord(persistable.Persistable):
    """ Audit record 
        TODO: Subset of access, not all of it!
    """

    access: permissions.Access

    @classmethod
    def from_access(cls, access: permissions.Access) -> typing.Self:
        audit = cls(access=access)
        return audit


class AuditPersistAction(persistable.SystemDataPersistAction):
    ...
