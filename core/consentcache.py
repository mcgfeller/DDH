
""" Cache mapping consent principals to keys. Used to provide consents API for looking up to what keys a principal has access to.
"""
from __future__ import annotations
import pydantic
import datetime
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from . import nodes, keys, transactions, common_ids, permissions
from backend import persistable


class _ConsentCache:
    """ Cache maping consent to key with modes
    """

    consents_by_principal: dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, permissions.AccessMode]]

    def __init__(self):
        self.consents_by_principal = {}

    def _clear(self):
        """ clear selective supports, for testing only """
        self.consents_by_principal.clear()
        return

    async def update(self, ddhkey: keys.DDHkey, added: set[permissions.Consent], removed: set[permissions.Consent]):
        return


ConsentCache = _ConsentCache()
