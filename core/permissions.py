""" DDH Core Access Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel,pyright_check
from . import errors



@pyright_check
class Principal(NoCopyBaseModel):
    """ Abstract identification of a party """

    id : str
    Delim : typing.ClassVar[str] = ','

    @classmethod
    def get_principals(cls, selection: str) -> list[Principal]:
        """ check string containling one or more Principals, separated by comma,
            return them as Principal.
            First checks CommonPrincipals defined here, then user_auth.UserInDB.
        """
        ids = selection.split(cls.Delim)
        principals = []
        for i in ids:
            p = CommonPrincipals.get(i)
            if not p:
                p = user_auth.UserInDB.load(id=i)
                assert p # load must raise error if not found
            principals.append(p)
        return principals

    def __eq__(self,other) -> bool:
        """ Principals are equal if their id is equal """
        return self.id == other.id if isinstance(other,Principal) else False

    def __hash__(self): 
        """ hashable on id """
        return hash(self.id)

    @classmethod
    def load(cls,id):
        raise errors.SubClass



AllPrincipal = Principal(id='_all_')
RootPrincipal = Principal(id='DDH')

# Collect all common principals
CommonPrincipals = {p.id : p for p in (AllPrincipal,RootPrincipal)}


@enum.unique
class AccessMode(str,enum.Enum):
    """ Access modes, can be used in a set. 
        We cannot use enum.Flag (which could be added), as pydantic doesn't support exporting / importing it as strings
    """
    read = 'read'
    protected = 'protected' # flag with read and write, mandatory if consented for write
    write = 'write'
    anonymous = 'anonymous'
    pseudonym = 'pseudonym'
    aggregated = 'aggregated'
    differential = 'differential' # differential privacy aggregation
    confidential = 'confidential' # confidential computing
    schema_read = 'schema_read'
    schema_write = 'schema_write'    
    consent_read = 'consent_read'
    consent_write = 'consent_write'

    def __repr__(self) -> str:
        """ more compact representation in messages and logs """
        return str.__str__(self)

    @classmethod
    def check(cls,requested :set[AccessMode], consented : set[AccessMode]) -> tuple[bool,str]:
        """ Check wether requsted modes are permitted by consented modes.
            There are two conditions:
            1.  All requested modes must be in consented modes; .RequiredModes do not count as
                consented.
            2.  If a mode in .RequiredModes is consented, it must be present in requested. 

        """
        # 1:
        for req in requested:
            if req not in consented and req not in AccessMode.RequiredModes : # type:ignore
                return False,f'requested mode {req} not in consented modes {", ".join(consented)}.'

        # 2:
        required_modes = consented.intersection(AccessMode.RequiredModes) # type:ignore # all modes required by our consent 
        for miss in required_modes - requested: # but not requested
            if m:= AccessMode.RequiredModes[miss]: # type:ignore # specific for a requested mode only?
                if m.isdisjoint(requested): # yes, but this mode is not requested, so check next miss
                    continue
            return False,f'Consent requires {miss} mode in request, but only {", ".join(requested)} requested.' 
        return True,'ok, with required modes' if required_modes else 'ok, no restrictions'

# modes that need to be specified explicity in requested when consented. If value is a set, the requirement only applies to the value modes:
AccessMode.RequiredModes = {AccessMode.anonymous : None, AccessMode.pseudonym : None, AccessMode.aggregated : None, # type:ignore
     AccessMode.confidential: None, AccessMode.differential: None, AccessMode.protected : {AccessMode.write}} 

@pyright_check
class User(Principal):
    """ Concrete user, may login """
       
    name : str 
    email : typing.Optional[pydantic.EmailStr] = None
    created_at : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now


@pyright_check
class DAppId(Principal):
    """ The identification of a DApp. We use a Principal for now. """

    name : str


@pyright_check
class Consent(NoCopyBaseModel):
    """ Consent to access a ressource denoted by DDHkey.
    """
    grantedTo : list[Principal]
    withApps : set[DAppId] = set()
    withModes : set[AccessMode]  = {AccessMode.read}


    def check(self,access : Access, _principal_checked=False) -> tuple[bool,str]:
        """ check access and return boolean and text explaining why it's not ok.
            If _principal_checked is True, applicable consents with correct principals 
            are checked, hence we don't need to double-check.
        """
        if (not _principal_checked) and self.grantedTo != AllPrincipal and access.principal not in self.grantedTo:
            return False,f'Consent not granted to {access.principal}'
        if self.withApps:
            if access.byDApp:
                if access.byDApp not in self.withApps:
                    return False,f'Consent not granted to DApp {access.byDApp}'
            else:
                return False,f'Consent granted to DApps; need an DApp id to access'
        
        ok,txt = AccessMode.check(access.modes,self.withModes)
        if not ok:
            return False,txt

        return True,'Granted by Consent; '+txt

@pyright_check
class Consents(NoCopyBaseModel):
    """ Multiple Consents, for one owner.
        If owner is not supplied, it is set to the Node's owner when
        the Node is created.
    """
    consents : list[Consent] = []
    _byPrincipal : dict[str,list[Consent]] = {}

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._byPrincipal = {} # for easier lookup
        for consent in self.consents:
            for principal in consent.grantedTo:
                cl = self._byPrincipal.setdefault(principal.id,[])
                cl.append(consent)
        return

    def applicable_consents(self,principal : Principal ) -> list[Consent]:
        """ return list of Consents for this principal """
        return self._byPrincipal.get(principal.id,[]) + self._byPrincipal.get(AllPrincipal.id,[])


    def check(self,owners : list[Principal], access : Access) -> tuple[bool,list[Consent],str]:
        msg = 'no consent'
        consent = None
        for consent in self.applicable_consents(access.principal):
            ok,msg = consent.check(access,_principal_checked=True)
            if ok:
                return ok,[consent],msg
        else:
            return False,[consent] if consent else [],msg

@pyright_check
class MultiOwnerConsents(NoCopyBaseModel):
    """ Records consents by different owners,
        check them all (they all must consent)
    """
    consents_by_owner : dict[Principal,Consents]


    def check(self,owners : list[Principal], access : Access) -> tuple[bool,list[Consent],str]:
        """ Check consents by all owner, only if all owners consent, we can go ahead.
        """
        msgs = []
        consents = []
        ok = False
        for owner in owners:
            ok,consent,msg = self.consents_by_owner[owner].check([owner],access)
            consents.append(consent)
            msgs.append(f'Owner {owner.id}: {msg}')
            if not ok:
                break # don't need to test others
        msgs = ('; '.join(msgs)) if msgs else 'no consent'
        return ok,consents,msgs


DDHkey = typing.ForwardRef('keys.DDHkey')

@enum.unique
class Operation(str,enum.Enum):
    """ allowed operations on keys """

    get = 'get'
    put = 'put'
    post = 'post'
    delete = 'delete'

    def __repr__(self): return self.value


class Access(NoCopyBaseModel):
    """ This is a loggable Access Request, which may or may not get fulfilled.
        Use .permitted() to check whether this request is permitted. 
    """
    op:        Operation = Operation.get
    ddhkey:    DDHkey # type: ignore
    principal: Principal
    byDApp:    typing.Optional[DAppId] = None
    modes:     set[AccessMode]  = {AccessMode.read}
    time:      datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now
    granted:   typing.Optional[bool] = None
    byConsent: typing.Optional[Consent] = None
    explanation: typing.Optional[str] = None

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self.ddhkey = self.ddhkey.ensure_rooted()
    
    def permitted(self,owner : typing.Optional[Principal] = None, record_access : bool = True) -> tuple[bool,typing.Optional[Consent],str]:
        """ checks whether access is permitted, returning (bool,required flags,applicable consent,explanation text)
            if record_access is set, the result is recorded into self.
        """
        consent = None
        if owner and owner == self.principal: # owner from key, remainder is owned by definition
            ok,msg = True,'principal is supplied owner'
        else:
            onode,split = keydirectory.NodeRegistry.get_node(self.ddhkey,nodes.NodeType.owner)
            if not onode:
                ok,msg = False,f'No owner node found for key {self.ddhkey}'
            elif onode.owners == (self.principal,):
                ok,msg = True,'Node owned by principal'
            else:
                consents : typing.Optional[Consents] = None
                if onode.consents: # onode has consents, use it
                    consents  = onode.consents
                else: # obtain from consents node
                    cnode,split = keydirectory.NodeRegistry.get_node(self.ddhkey,nodes.NodeType.consents) 
                    if cnode:
                        consents = typing.cast(Consents,cnode.consents)  # consent is not None by get_node
                if consents:
                    ok,consent,msg = consents.check(onode.owners,self) # check consents
                else:
                    ok,msg =  False,f'Owner is not accessor, and no consent node found for key {self.ddhkey}'

        if record_access:
            self.granted = ok
            self.explanation = msg
            self.byConsent = consent
        return  ok,consent,msg


    
    def audit_record(self) -> dict:
        return {}


from . import keys
from . import nodes
from . import keydirectory
from frontend import user_auth
Access.update_forward_refs()