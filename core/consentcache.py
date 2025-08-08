
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


class ConsentCacheEntry(DDHbaseModel):
    """ Entry for one DDHkey in ConsentCache """

    modes: set[permissions.AccessMode]


class _ConsentCache:
    """ Cache maping consent to key with modes.

        Note:   We intentionally do not keep Consent objects, as they reveal consentees other than the principal,
                and we don't want to leak this information.
    """

    consents_by_principal: dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, ConsentCacheEntry]]

    def __init__(self):
        self.consents_by_principal = {}

    def _clear(self):
        """ clear selective supports, for testing only """
        self.consents_by_principal.clear()
        return

    async def update(self, ddhkey: keys.DDHkeyGeneric, added: frozenset[permissions.Consent], removed: frozenset[permissions.Consent]) -> dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, set[permissions.AccessMode]]]:
        """ Update the cache with added and removed consents. Return {prinicpal: {key: modes}} for added keys only
        """
        # remove first, removal is more involved than adding
        newkeys = {}
        for c in removed:
            for p in c.grantedTo:
                if (g := self.consents_by_principal.get(p.id, None)):
                    if (s := g.get(ddhkey)):
                        s.modes -= c.withModes  # discard modes
                        if not s.modes:
                            g.pop(ddhkey)  # remove empty
                    if not g:
                        self.consents_by_principal.pop(p.id, None)  # remove empty
        for c in added:
            for p in c.grantedTo:
                modes = c.withModes.copy()  # withModes must not be shared
                self.consents_by_principal.setdefault(p.id, {})[ddhkey] = ConsentCacheEntry(modes=modes)
                newkeys.setdefault(p.id, {})[ddhkey] = modes
        return newkeys

    def as_consents_for(self, principal: principals.Principal) -> dict[keys.DDHkeyGeneric, permissions.Consents]:
        """ Return Consents received by a Principal, as Consents object. Omit empty modes """
        cs = self.consents_by_principal.get(principal.id, {})
        consents = {k: permissions.Consent.single(
            grantedTo=[principal], withModes=c.modes) for k, c in cs.items() if c}
        return consents


ConsentCache = _ConsentCache()
