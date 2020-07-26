""" DDH Core Models """

import pydantic 
import datetime
import typing
import enum

class NoCopyBaseModel(pydantic.BaseModel):
    """ https://github.com/samuelcolvin/pydantic/issues/1246
        https://github.com/samuelcolvin/pydantic/blob/52af9162068a06eed5b84176e987a534f6d9126a/pydantic/main.py#L574-L575
    """

    @classmethod
    def validate(cls: typing.Type[pydantic.BaseModel], value: typing.Any) -> pydantic.BaseModel:
        if isinstance(value, cls):
            return value # don't copy!
        else:
            return pydantic.BaseModel.validate(cls, value)

class Principal(NoCopyBaseModel):

    id : int


AllPrincipal = Principal(id=0)




@enum.unique
class AccessMode(str,enum.Enum):
    """ Access modes, can be added """
    read = 'read'
    read_for_write = 'read_for_write' # read with the intention to write data back   
    write = 'write'
    anonymous = 'anonymous'
    pseudonym = 'pseudonym'


@enum.unique
class AccessModeF(enum.Flag):
    """ Access modes as enum.intflag - pydantic doesn't support export / import as strings """
    read = enum.auto()
    write = enum.auto()
    read_for_write = enum.auto()
    anonymous = enum.auto()
    pseudonym = enum.auto()


class User(Principal):

       
    name : str 
    email : pydantic.EmailStr = None
    created_at : datetime.datetime = None

class DAppId(Principal):
    """ The identification of a DApp. We use a Principal for now. """

    name : str


class Consent(NoCopyBaseModel):
    """ Consent to access a ressource denoted by DDHkey.
    """
    grantedTo : typing.List[Principal]
    withApps : typing.List[DAppId] = []
    withMode : typing.List[AccessMode]  = [AccessMode.read]

    def check(self,access : 'Access') -> typing.Tuple[bool,str]:
        return False,'not checked'

class DDHkey(NoCopyBaseModel):
    
    key = str
    owner: Principal
    consent : Consent = None

    @classmethod
    def get_key(cls,path : str) -> typing.Optional['DDHkey']:
        """ get key from path string """
        user = User(id=1,name='martin',email='martin.gfeller@swisscom.com')
        ddhkey = DDHkey(key='unknown',owner=user)
        return ddhkey

    def get_schema_parent(self) -> 'DDHkey':
        """ get key up the tree where we have a schema """
        return self

    def get_consent_parent(self) -> 'DDHkey':
        """ get key up the tree where we have a consent """
        return self

class Access(NoCopyBaseModel):
    """ This is a loggable Access Request, which may or may not get fulfilled.
        Use .permitted() to check whether this request is permitted. 
    """
    ddhkey:    DDHkey
    principal: Principal
    mode:      typing.List[AccessMode]  = [AccessMode.read]
    #mode:      AccessModeF  = AccessModeF.read
    time:      datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now
    
    def permitted(self) -> bool:
        if self.ddhkey.owner == self.principal:
            return True
        elif self.ddhkey.consent:
            ok,msg = self.ddhkey.consent.check(self)
        else:
            return False
    
    def audit_record(self) -> dict:
        return {}




class Schema(NoCopyBaseModel): ...



class _SchemaRegistry(NoCopyBaseModel):
    """ Singleton SchemaRegistry """


    def __init__(self,**kw):
        super().__init__(**kw)

    def get_schema_for(self,ddhkey: DDHkey) -> typing.Optional[Schema]:
        return None

    def put_schema_for(self,ddhkey: DDHkey,schema: Schema):
        return


SchemaRegistry = _SchemaRegistry() 