""" DDH Core Access Models """
from __future__ import annotations
import pydantic
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel, utcnow
from core import errors, principals, node_types, common_ids


@enum.unique
class AccessMode(str, enum.Enum):
    """ Access modes, can be used in a set. 
        We cannot use enum.Flag (which could be added), as pydantic doesn't support exporting / importing it as strings
    """
    read = 'read'
    protected = 'protected'  # flag with read and write, mandatory if consented for write
    write = 'write'
    combined = 'combined'
    anonymous = 'anonymous'
    pseudonym = 'pseudonym'
    aggregated = 'aggregated'
    differential = 'differential'  # differential privacy aggregation
    confidential = 'confidential'  # confidential computing
    login = 'login'

    def __repr__(self) -> str:
        """ more compact representation in messages and logs """
        return str.__str__(self)

    @classmethod
    def check(cls, requested: set[AccessMode], consented: set[AccessMode]) -> tuple[bool, str]:
        """ Check wether requsted modes are permitted by consented modes.
            There are two conditions:
            1.  All requested modes must be in consented modes; .RequiredModes do not count as
                consented.
            2.  If a mode in .RequiredModes is consented, it must be present in requested. 

        """
        # 1:
        for req in requested:
            if req not in consented and req not in AccessMode.RequiredModes:  # type:ignore
                return False, f'requested mode {req} not in consented modes {", ".join(consented)}.'

        # 2:
        # type:ignore # all modes required by our consent
        required_modes = consented.intersection(AccessMode.RequiredModes)
        for miss in required_modes - requested:  # but not requested
            if m := AccessMode.RequiredModes[miss]:  # type:ignore # specific for a requested mode only?
                if m.isdisjoint(requested):  # yes, but this mode is not requested, so check next miss
                    continue
            return False, f'Consent requires {miss} mode in request, but only {", ".join(requested)} requested.'
        return True, 'ok, with required modes' if required_modes else 'ok, no restrictions'


# modes that need to be specified explicity in requested when consented. If value is a set, the requirement only applies to the value modes:
AccessMode.RequiredModes = {AccessMode.anonymous: None, AccessMode.pseudonym: None, AccessMode.aggregated: None,  # type:ignore
                            AccessMode.confidential: None, AccessMode.differential: None, AccessMode.protected: {AccessMode.write}}


class Consent(DDHbaseModel):
    """ Consent to access a resource denoted by DDHkey.
    """
    grantedTo: list[principals.Principal | common_ids.PrincipalId]  # will be converted to Principal in validator
    withApps: set[principals.DAppId] = set()
    withModes: set[AccessMode] = {AccessMode.read}
    withinDates: DateRestriction | None = None

    @pydantic.field_validator('grantedTo', mode='after')
    def to_principal(v):
        """ convert str value to Principal """
        v = [principals.Principal(id=p) if isinstance(p, str) else p for p in v]
        return v

    def check(self, access: Access, _principal_checked=False) -> tuple[bool, str]:
        """ check access and return boolean and text explaining why it's not ok.
            If _principal_checked is True, transformer consents with correct principals 
            are checked, hence we don't need to double-check.
        """
        if (not _principal_checked) and self.grantedTo != principals.AllPrincipal and access.principal not in self.grantedTo:
            return False, f'Consent not granted to {access.principal}'
        if self.withApps:
            if access.byDApp:
                if access.byDApp not in self.withApps:
                    return False, f'Consent not granted to DApp {access.byDApp}'
            else:
                return False, f'Consent granted to DApps; need an DApp id to access'

        ok, txt = AccessMode.check(access.modes, self.withModes)
        if not ok:
            return False, txt

        if self.withinDates:
            ok, txt2 = self.withinDates.check(access.time)
            txt += ', '+txt2
            access.consent_expiry = self.withinDates.end  # Record consent expiry date
            if not ok:
                return False, txt2

        return True, 'Granted by Consent; '+txt

    def __hash__(self):
        """ make Consents hashable """
        return hash(self._as_tuple())

    def __eq__(self, other):
        if isinstance(self, Consent):
            return self._as_tuple() == other._as_tuple()
        else:
            return False

    def _as_tuple(self):
        """ return hashable tuple with all attributes that are distinct """
        return (tuple(self.grantedTo), frozenset(self.withApps), frozenset(self.withModes), self.withinDates.as_tuple() if self.withinDates else None)

    @classmethod
    def single(cls, *a, **kw) -> Consents:
        """ Create Consents from arguments of Consent; for lazy typers """
        return Consents(consents=[cls(*a, **kw)])


class DateRestriction(DDHbaseModel):
    """ Date range within which the consent is valid. Can be given as begin (default now) to end range, or 
        begin and days.
    """
    begin: datetime.datetime = pydantic.Field(default_factory=utcnow)
    end: datetime.datetime | None = None
    days: typing.Annotated[int, pydantic.Field(gt=0, le=366)] | None = None

    @pydantic.model_validator(mode='after')
    def set_end(self) -> typing.Self:
        """ Set end date from days, raise error if neither end nor days or both and and days are specified.

            Pydantic issue https://github.com/pydantic/pydantic/issues/12085: Validator is called twice
        """
        if self.end is None:
            if self.days is not None:
                self.end = self.begin + datetime.timedelta(days=self.days)
                self.days = None  # Pydantic does call this validator twice, so avoid 2nd check causing errors
            else:
                raise ValueError('either end or days is required')
        elif self.days is not None:
            raise ValueError('end and days cannot be both supplied')

        return self

    def check(self, access_date: datetime.datetime) -> tuple[bool, str]:
        assert access_date.tzinfo is not None, 'access.time must not be naive datetime'
        if access_date < self.begin:
            return False, f'Access must not be before {self.begin}'
        assert self.end
        if access_date > self.end:
            return False, f'Access must be before {self.end}'
        return True, f'Access expires at {self.end}'

    def as_tuple(self) -> tuple[datetime.datetime]:
        return (self.begin, self.end)  # type:ignore


class Consents(DDHbaseModel):
    """ Multiple Consents, for one owner.
        If owner is not supplied, it is set to the Node's owner when
        the Node is created.
    """
    consents: list[Consent] = []
    _byPrincipal: dict[str, list[Consent]] = {}

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._byPrincipal = {}  # for easier lookup
        for consent in self.consents:
            for principal in consent.grantedTo:
                cl = self._byPrincipal.setdefault(principal.id, [])
                cl.append(consent)
        return

    def consentees(self) -> set[principals.Principal]:
        """ all principals that enjoy some sort of Consent """
        return set(sum([c.grantedTo for c in self.consents], []))

    def consentees_with_mode(self, mode: AccessMode) -> set[principals.Principal]:
        """ all principals that enjoy Consent of mode """
        return set(sum([c.grantedTo for c in self.consents if mode in c.withModes], []))

    def transformer_consents(self, principal: principals.Principal) -> list[Consent]:
        """ return list of Consents for this principal """
        return self._byPrincipal.get(principal.id, []) + self._byPrincipal.get(principals.AllPrincipal.id, [])

    def check(self, owners: typing.Iterable[principals.Principal], access: Access) -> tuple[bool, list[Consent], str]:
        msg = 'no consent'
        consent = None
        for consent in self.transformer_consents(access.principal):
            ok, msg = consent.check(access, _principal_checked=True)
            if ok:
                return ok, [consent], msg
        else:
            return False, [consent] if consent else [], msg

    def changes(self, new_consents: Consents) -> tuple[frozenset[Consent], frozenset[Consent]]:
        """ return added and removed consents as (set(),set()) """
        old = frozenset(self.consents); new = frozenset(new_consents.consents)
        return (new-old, old-new)


DefaultConsents = Consents(consents=[])


class MultiOwnerConsents(DDHbaseModel):
    """ Records consents by different owners,
        check them all (they all must consent)
    """
    consents_by_owner: dict[principals.Principal, Consents]

    def check(self, owners: typing.Iterable[principals.Principal], access: Access) -> tuple[bool, list[Consent], str]:
        """ Check consents by all owner, only if all owners consent, we can go ahead.
        """
        msgs = []
        consents = []
        ok = False
        for owner in owners:
            ok, consent, msg = self.consents_by_owner[owner].check([owner], access)
            consents.append(consent)
            msgs.append(f'Owner {owner.id}: {msg}')
            if not ok:
                break  # don't need to test others
        msgs = ('; '.join(msgs)) if msgs else 'no consent'
        return ok, consents, msgs

    def consentees(self) -> set[principals.Principal]:
        """ all principals that enjoy some sort of Consent """
        return set.intersection(*[c.consentees() for c in self.consents_by_owner.values()])

    def consentees_with_mode(self, mode: AccessMode) -> set[principals.Principal]:
        """ all principals that enjoy Consent of mode """
        return set.intersection(*[c.consentees_with_mode(mode) for c in self.consents_by_owner.values()])


DDHkey = typing.ForwardRef('keys.DDHkey')


@enum.unique
class Operation(str, enum.Enum):
    """ allowed operations on keys """

    get = 'get'
    put = 'put'
    post = 'post'
    delete = 'delete'

    def __repr__(self): return self.value


class Access(DDHbaseModel):
    """ This is a loggable Access Request, which may or may not get fulfilled.
        Use .permitted() to check whether this request is permitted. 
    """
    op:        Operation = Operation.get
    ddhkey:    DDHkey  # type: ignore
    original_ddhkey: DDHkey | None = None  # type: ignore # original key if ddhkey is modified in pseudonymous access
    principal: principals.Principal | None = None  # will be set once added to a trx
    byDApp:    principals.DAppId | None = None
    modes:     set[AccessMode] = {AccessMode.read}
    time:      datetime.datetime | None = None  # Time is set in __init__ to protect from supplying fake time
    granted:   bool | None = None
    byConsents: list[Consent] = []
    explanation: str | None = None
    consent_expiry: datetime.datetime | None = None
    schema_key_split: int | None = pydantic.Field(
        default=None, description="Split into DDHKey for schema and path into schema")
    data_key_split: int | None = pydantic.Field(
        default=None, description="Split into DDHKey for data and path into data node")
    failed: str | None = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.original_ddhkey = self.ddhkey = self.ddhkey.ensure_rooted()
        self.time = utcnow()  # always set time

    def include_mode(self, mode: AccessMode):
        """ ensure that mode is included in access modes """
        self.modes.add(mode)

    def permitted(self, node: node_types.T_Node | None, owner: principals.Principal | None = None, record_access: bool = True) -> tuple[bool, list[Consent], set[Principal], str]:
        """ checks whether access is permitted, returning (bool,required flags,transformer consent,explanation text)
            if record_access is set, the result is recorded into self.
        """
        if self.principal is None:
            raise errors.AccessError('Add Access to Transaction first.')
        used_consents = []
        consentees = set()
        if owner is not None:
            keyowners = (owner,)
        else:
            keyowners = user_auth.get_principals(self.ddhkey.owner)

        if not node:  # cannot use this test when a MultiOwnerNode is given!
            # single owner from key, remainder is owned by definition
            if len(keyowners) == 1 and self.principal == keyowners[0]:
                ok, msg = True, 'principal is key owner'
                consentees = {self.principal}
            else:  # no transformer node, and keyowner is not principal accessor!
                ok, msg = False, f'No data/consent node found for key {self.ddhkey}'
        else:  # we have a node
            if (self.principal,) == node.owners:  # single owner == principal
                ok, msg = True, 'Node owned by principal'
                consentees = {self.principal}
            else:
                if node.consents:
                    ok, used_consents, msg = node.consents.check(node.owners, self)  # check consents
                    consentees = node.consents.consentees_with_mode(AccessMode.read) | {self.principal}
                else:
                    ok, msg = False, f'Owner is not accessor, and no consent found for key {self.ddhkey}'

        if AccessMode.read not in self.modes:  # consentees are for read only
            consentees = set()

        if record_access:
            self.granted = ok
            self.explanation = msg
            self.byConsents = used_consents
        return ok, used_consents, consentees, msg

    def raise_if_not_permitted(self, node: node_types.T_Node | None, owner: principals.Principal | None = None, record_access: bool = True):
        """ raise access error if this access to node is not permitted """
        ok, used_consents, consentees, msg = self.permitted(node)
        if not ok:
            raise errors.AccessError(msg)
        return ok, used_consents, consentees, msg

    def remainder(self) -> keys.DDHkey:
        """ return remainder key """
        if self.schema_key_split is None:
            return self.ddhkey
        else:
            return self.ddhkey.remainder(self.schema_key_split)


from . import keys
from frontend import user_auth
Access.model_rebuild()
