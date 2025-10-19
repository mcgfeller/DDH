
""" Cache mapping consent principals to keys. Used to provide consents API for looking up to what keys a principal has access to.
"""

import pydantic
import datetime
import typing
import secrets


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel, CV, utcnow

from . import nodes, keys, transactions, common_ids, permissions, common_ids, principals, errors
from backend import persistable


class ConsentCacheEntry(DDHbaseModel):
    """ Entry for one DDHkey in ConsentCache """

    modes: set[permissions.AccessMode]
    _by_principal: dict[common_ids.PrincipalId, common_ids.PrincipalId] = {}  # secrets per principal id
    _by_secret: dict[common_ids.PrincipalId, common_ids.PrincipalId] = {}  # principal id by secret
    _expiration: datetime.datetime | None = None  # expiration of the secret

    # Time to live - longer pseudonymous, to give a chance to reply:
    TTL_anon: CV[datetime.timedelta] = datetime.timedelta(hours=3)
    TTL_pseudo: CV[datetime.timedelta] = datetime.timedelta(days=10)
    ID_prefix: CV[str] = '_A_'
    ID_len: CV[int] = 10

    def get_secret(self, principal_id: common_ids.PrincipalId) -> common_ids.PrincipalId:
        """ get a secret key for a principal. 
            Use it unless it has expired, or create a new one. 
        """
        now = utcnow()
        if (not self._by_principal) or now > self._expiration:
            ttl = self.TTL_pseudo if permissions.AccessMode.pseudonym in self.modes else self.TTL_anon
            self._expiration = now + ttl
            self._by_principal = {}
            self._by_secret = {}
        if not (s := self._by_principal.get(principal_id)):
            s = self._by_principal[principal_id] = common_ids.PrincipalId(
                self.ID_prefix + secrets.token_urlsafe(self.ID_len))
            self._by_secret[s] = principal_id
        return s

    @property
    def is_anon(self) -> bool:
        return any(mode in self.modes for mode in (permissions.AccessMode.pseudonym, permissions.AccessMode.anonymous))

    def anon_key(self, key: keys.DDHkey) -> keys.DDHkey:
        """ if grant is anonymized, return anonymized key, else unchanged key """
        if self.is_anon:
            key = key.with_new_owner(self.get_secret(key.owner))
        return key


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

    def update(self, ddhkey: keys.DDHkeyGeneric, added: typing.Iterable[permissions.Consent], removed: typing.Iterable[permissions.Consent]) -> dict[common_ids.PrincipalId, dict[keys.DDHkeyGeneric, set[permissions.AccessMode]]]:
        """ Update the cache with added and removed consents. Return {prinicpal: {key: modes}} for added keys only.
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
                cce = ConsentCacheEntry(modes=modes)
                a_key = cce.anon_key(ddhkey)
                self.consents_by_principal.setdefault(p.id, {})[a_key] = cce
                newkeys.setdefault(p.id, {})[a_key] = modes
        return newkeys

    def as_consents_for(self, principal: principals.Principal) -> dict[keys.DDHkeyGeneric, permissions.Consents]:
        """ Return Consents received by a Principal, as Consents object. Omit empty modes """
        cs = self.consents_by_principal.get(principal.id, {})
        consents = {k: permissions.Consent.single(
            grantedTo=[principal], withModes=c.modes) for k, c in cs.items() if c}
        return consents

    def get_real_key(self, trx_owner: principals.Principal, orig_key: keys.DDHkey) -> keys.DDHkey:
        """ replace the anon principle in the orig_key by the true principle, looking up in ConsentCache for 
            trx_owner. We allow real key owner if it matches the trx owner. 
        """
        if trx_owner.id == orig_key.owner:  # owner itself asks for their data.
            key = orig_key
        else:
            cc = self.consents_by_principal.get(trx_owner.id)
            if not cc:
                raise errors.AccessError(
                    f'Anonymous key invalid: {orig_key}; nothing consented to {trx_owner.id}')
            gkey = orig_key.without_variant_version()
            if not (cce := cc.get(gkey)):
                raise errors.AccessError(
                    f'Anonymous key invalid: {orig_key}; key not consented.')
            if not (real_owner := cce._by_secret.get(orig_key.owner)):
                raise errors.AccessError(
                    f'Anonymous key expired: {orig_key}')
            key = orig_key.with_new_owner(real_owner)
        return key


ConsentCache = _ConsentCache()
