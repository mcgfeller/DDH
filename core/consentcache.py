
""" Cache mapping consent principals to keys. Used to provide consents API for looking up to what keys a principal has access to.
"""
from __future__ import annotations
import pydantic
import datetime
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from . import nodes, keys, transactions, common_ids, permissions, common_ids, principals
from backend import persistable


class _ConsentCache:
    """ Cache maping consent to key with modes.

        Note:   We intentionally do not keep Consent objects, as they reveal consentees other than the principal,
                and we don't want to leak this information.
    """

    consents_by_principal: dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, set[permissions.AccessMode]]]

    def __init__(self):
        self.consents_by_principal = {}

    def _clear(self):
        """ clear selective supports, for testing only """
        self.consents_by_principal.clear()
        return

    async def update(self, ddhkey: keys.DDHkeyGeneric, added: frozenset[permissions.Consent], removed: frozenset[permissions.Consent]):
        # remove first, removal is more involved than adding
        for c in removed:
            for p in c.grantedTo:
                if (g := self.consents_by_principal.get(p.id, None)):
                    if (s := g.get(ddhkey)):
                        s -= c.withModes  # discard modes
                        if not s:
                            g.pop(ddhkey)  # remove empty
                    if not g:
                        self.consents_by_principal.pop(p.id, None)  # remove empty
        for c in added:
            for p in c.grantedTo:
                self.consents_by_principal.setdefault(p.id, {})[ddhkey] = c.withModes
        return

    def as_consents_for(self, principal: principals.Principal) -> dict[keys.DDHkeyGeneric, permissions.Consents]:
        """ Return Consents received by a Principal, as Consents object """
        cs = self.consents_by_principal.get(principal.id, {})
        consents = {k: permissions.Consent.single(grantedTo=[principal], withModes=c) for k, c in cs.items()}
        return consents


ConsentCache = _ConsentCache()
