""" DDH Core Models """
from __future__ import annotations
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
            return super().validate(value) 


class Principal(NoCopyBaseModel):

    id : str


AllPrincipal = Principal(id='_all_')
RootPrincipal = Principal(id='DDH')




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
    email : typing.Optional[pydantic.EmailStr] = None
    created_at : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now

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

class _RootType:
    """ Singleton root marker """
    def __repr__(self):
        return '<Root>'

class DDHkey(NoCopyBaseModel):
    
    key : tuple

    node: typing.Optional[Node] = None

    Delimiter : typing.ClassVar[str] = '/'
    Root : typing.ClassVar[_RootType] = _RootType()

    def __init__(self,key : typing.Union[tuple,list,str], node :  typing.Optional['Node'] = None):
        """ Convert key string into tuple, eliminate empty segments, and set root to self.Root """
        if isinstance(key,str):
            key = key.split(self.Delimiter)
        if len(key) == 0:
            raise ValueError('Key may not be empty')
        elif not key[0]: # replace root with key indicator
            key = (self.Root,)+tuple(filter(None,key[1:]))
        else:
            key = tuple(filter(None,key))
        super().__init__(key=key,node=node)
        return 


    def up(self) -> typing.Optional['DDHkey']:
        """ return key up one level, or None if at top """
        upkey = self.key[:-1]
        if upkey:
            return self.__class__(upkey)
        else: 
            return None

    # def execute(self,  user: Principal, q : str):
    #     np = self.get_node_parent()
    #     return np.execute(user,q)

   




class Access(NoCopyBaseModel):
    """ This is a loggable Access Request, which may or may not get fulfilled.
        Use .permitted() to check whether this request is permitted. 
    """
    ddhkey:    DDHkey
    principal: Principal
    mode:      typing.List[AccessMode]  = [AccessMode.read]
    #mode:      AccessModeF  = AccessModeF.read
    time:      datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now
    
    def permitted(self) -> typing.Tuple[bool,str]:
        onode = NodeRegistry.get_node(self.ddhkey,NodeType.owner)
        if not onode:
            return False,f'No owner node found for key {self.ddhkey}'
        elif onode.owner == self.principal:
            return True,'Node owned by principal'
        else:
            if onode.consent: # onode has consent, use it
                consent : Consent = onode.consent
            else: # obtain from consent node
                cnode = NodeRegistry.get_node(self.ddhkey,NodeType.consent) 
                if cnode:
                    consent = typing.cast(Consent,cnode.consent)  # consent is not None by get_node
                else:
                    return False,f'Owner is not accessor, and no consent node found for key {self.ddhkey}'
            ok,msg = consent.check(self) # check consent
            return ok,msg

    
    def audit_record(self) -> dict:
        return {}




class Schema(NoCopyBaseModel): ...


@enum.unique
class NodeType(str,enum.Enum):
    """ Types of Nodes, marked by presence of attribute corresponding with enum value """

    owner = 'owner'
    nschema = 'nschema'
    consent = 'consent'
    data = 'data'


class Node(NoCopyBaseModel):

    owner: Principal
    consent : typing.Optional[Consent] = None
    nschema : typing.Optional[Schema] = pydantic.Field(alias='schema')

    """ node at DDHkey """
    def execute(self,  user: Principal, q : str):
        return {}

    def defineKey(self,ddhkey: DDHkey):
        return

class DAppNode(Node):
    """ node managed by a DApp """
    ...

class StorageNode(Node):
    """ node with storage on DDH """
    ...

DDHkey.update_forward_refs() # Now Node is known

class _NodeRegistry:
    """ Preliminary holder of nodes """

    nodes_by_key : typing.Dict[tuple,Node]

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : DDHkey, node: Node):
        self.nodes_by_key[key.key] = node

    def __getitem__(self,key : DDHkey) -> typing.Optional[Node]:
        return self.nodes_by_key.get(key.key,None) 

    def get_next_node(self,key : typing.Optional[DDHkey]) -> typing.Iterator[Node]:
        """ Generating getting next node walking up the tree from key.
            """
        while key:
            node =  self[key]
            key = key.up() 
            if node:
                yield node
        else:
            return

    def get_node(self,key : DDHkey,node_type : NodeType) -> typing.Optional[Node]:
        """ get closest (upward-bound) node which has nonzero attribute """
        node = next((n for n in self.get_next_node(key) if getattr(n,node_type.value,None)),None)
        return node
    

NodeRegistry = _NodeRegistry()

NodeRegistry[DDHkey((DDHkey.Root,))] = Node(owner=RootPrincipal)
