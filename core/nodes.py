
""" DDH Core Node Models """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import datetime
import typing
import enum
import abc
import secrets
import zlib

from pydantic.errors import PydanticErrorMixin, SubclassError
from utils.pydantic_utils import NoCopyBaseModel


from . import permissions,schemas,transactions,errors
from utils import datautils




@enum.unique
class NodeType(str,enum.Enum):
    """ Types of Nodes, marked by presence of attribute corresponding with enum value """

    owner = 'owner'
    nschema = 'nschema'
    consents = 'consents'
    data = 'data'
    execute = 'execute'

    def __repr__(self): return self.value

@enum.unique
class Ops(str,enum.Enum):
    """ Operations """

    get     = 'get'
    post    = 'post'
    put     = 'put'
    delete  = 'delete'


    def __repr__(self): return self.value

@enum.unique
class DataFormat(str,enum.Enum):
    """ Operations """

    dict = 'd'
    blob = 'b'
    json = 'j'    

NodeId = typing.NewType('NodeId', str)

from backend import keyvault,storage




class Persistable(NoCopyBaseModel):
    """ class that provides methods to get persistent state.
        Works with storage.
    """

    Registry : typing.ClassVar[dict[str,type]] = {}
    id : NodeId = pydantic.Field(default_factory=secrets.token_urlsafe)
    format : DataFormat = DataFormat.dict

    @classmethod
    def __init_subclass__(cls):
        Persistable.Registry[cls.__name__] = cls
 

    def store(self, transaction: transactions.Transaction ):
        d = self.to_compressed()
        storage.Storage.store(self.id,d, transaction)
        return

    @classmethod
    def load(cls, id:NodeId,  transaction: transactions.Transaction ):
        d = storage.Storage.load(id, transaction)
        o = cls.from_compressed(d)
        return o

    def delete(self, transaction: transactions.Transaction ):
        self.__class__.load(self.id, transaction) # verify encryption by loading
        storage.Storage.delete(self.id, transaction)
        return

    def to_compressed(self) -> bytes:
        return zlib.compress(self.to_json().encode(), level=-1)

    @classmethod
    def from_compressed(cls,data : bytes):
        return cls.from_json(zlib.decompress(data).decode()).data

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_json(cls, j :str) -> Persistable:
        o = cls.parse_raw(j)
        return o

    def get_key(self):
        raise NotImplementedError()

    def ensure_loaded(self, transaction : transactions.Transaction) -> Persistable:
        return self

    def get_proxy(self) -> PersistableProxy:
        return PersistableProxy(id=self.id,cls=self.__class__.__name__)

class PersistableProxy(NoCopyBaseModel):
    """ Proxy with minimal data to load Persistable """
    id : NodeId
    cls: str

    def ensure_loaded(self, transaction : transactions.Transaction) -> Persistable:
        cls = Persistable.Registry[self.cls]
#        cls = typing.cast(Persistable,cls)
        obj = cls.load(self.id,transaction)
        assert isinstance(obj,cls)
        return obj





class Node(Persistable):

    types: set[NodeType] = set() # all supported type, will be filled by init unless given
    owner: permissions.Principal
    consents : typing.Optional[permissions.Consents] = None
    nschema : typing.Optional[schemas.Schema] =  pydantic.Field(alias='schema')
    key : typing.Optional[keys.DDHkey] = None

    def __init__(self,**data):
        """ .types will be filled based on attributes that are not Falsy """
        super().__init__(**data)
        if not self.types:
            self.types = {t for t in NodeType if getattr(self,t.value,None)}
        return

    def __str__(self):
        """ short representation """
        return f'Node(types={self.types!s},key={self.key!s},owner={self.owner.id})'


    def get_sub_schema(self, ddhkey: keys.DDHkey,split: int, schema_type : str = 'json') -> typing.Optional[schemas.Schema]:
        """ return schema based on ddhkey and split """
        s = typing.cast(schemas.Schema,self.nschema)
        s = s.obtain(ddhkey,split)
        return s

    @property
    def owners(self) -> tuple[permissions.Principal,...]:
        """ get one or multiple owners """
        return (self.owner,)


        


from . import keys # avoid circle
Node.update_forward_refs() # Now Node is known, update before it's derived


class MultiOwnerNode(Node):

    all_owners : tuple[permissions.Principal,...]
    consents : typing.Union[permissions.Consents,permissions.MultiOwnerConsents,None] = None

    def __init__(self,**data):
        data['owner'] = data.get('all_owners',(None,))[0] # first owner, will complain in super
        super().__init__(**data)
        if isinstance(self.consents,permissions.Consents): # Convert Consents into MultiOwnerConsents:
            self.consents = permissions.MultiOwnerConsents(consents_by_owner={self.owner: self.consents})
        elif self.consents: # sanity check, Consents owners must be node owners
            d = set(self.consents.consents_by_owner.keys())-set(self.all_owners)
            if d:
                raise ValueError(f'Following Consent owners must be Node owners: {d}')
        return

    @property
    def owners(self) -> tuple[permissions.Principal,...]:
        """ get one or multiple owners """
        return self.all_owners


class ExecutableNode(Node):
    """ A node that provides for execution capabilities """

    type: typing.ClassVar[NodeType] = NodeType.execute

    @abstractmethod
    def execute(self, op: Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        return {}


class DelegatedExecutableNode(ExecutableNode):
    """ A node that delegates executable methods to DApps """

    executors : list = []

    def execute(self, op: Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        d = None
        for executor in self.executors:
            d = executor.get_and_transform(access,key_split, q)
        return d


from backend import storage,keyvault






class DataNode(Persistable):
    """ New data node, points to storage and consents """

    owner: permissions.Principal
    key : typing.Optional[keys.DDHkey] = None

    format : DataFormat = DataFormat.dict
    data : typing.Any        
    _consents : permissions.Consents = permissions.DefaultConsents
    storage_loc : typing.Optional[NodeId] = None
    access_key: typing.Optional[keyvault.AccessKey] = None



    @property
    def consents(self):
        return self._consents

    @consents.setter
    def consents(self, value):
        """ We want to make clear that this is an expensive operation, not just a param """
        raise NotImplementedError('use .change_consents()')

    def store(self, transaction: transactions.Transaction ):
        d = self.to_compressed()
        if self.id not in storage.Storage:
            keyvault.set_new_storage_key(self,transaction.for_user,[],[])
        enc = keyvault.encrypt_data(transaction.for_user,self,d)
        storage.Storage.store(self.id,enc, transaction)
        return

    @classmethod
    def load(cls, id:NodeId,  transaction: transactions.Transaction ):
        enc = storage.Storage.load(id,transaction)
        plain = keyvault.decrypt_data(transaction.for_user,self,enc)
        o = cls.from_compressed(plain)
        return o




    def execute(self, op: Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        if key_split:
            top,remainder = access.ddhkey.split_at(key_split)
            if self.format != DataFormat.dict:
                raise errors.NotSelectable(remainder)
        if op == Ops.get:
            if key_split:
                self.data = datautils.extract_data(self.data,remainder,raise_error=errors.NotFound)
        elif op == Ops.put:
            assert data is not None
            if key_split:
                self.data = datautils.insert_data(self.data,remainder,data,raise_error=errors.NotFound)
            self.store(transaction)
        elif op == Ops.delete:
            self.delete(transaction)

        return data

    def update_consents(self,access : permissions.Access, transaction: transactions.Transaction, remainder: keys.DDHkey, consents: permissions.Consents):
        """ update consents at remainder key.
            Data must be read using previous encryption and rewritten using the new encryption. See 
            section 7.3 "Protection of data at rest and on the move" of the DDH paper.
        """
        assert self.key
        if self.consents: # had consents before, check changes:
            added,removed = self.consents.changes(consents)
            effective = consents.consentees()
        else: # all new
            added = effective = consents.consentees() ; removed = []

        if added or removed: # expensive op follows, do only if something has changed
            self._consents = consents # actually update
            prev_data = self.data  # need before new key is generated          
            if remainder.key: # change is not at this level, insert a new node:
                if self.format != DataFormat.dict:
                    raise errors.NotSelectable(remainder)
                key=keys.DDHkey(key=self.key.key+remainder.key)
                above,below = datautils.split_data(prev_data,remainder,raise_error=errors.NotFound) # if we're deep in data
                node = self.__class__(owner=self.owner,key=key,_consents=consents,data=below)
                self.data = above
            else:
                above =  None
                node = self # top level

                
            keyvault.set_new_storage_key(node,access.principal,effective,removed) # now we can set the new key

            node.store(transaction) # re-encrypt on new node (may be self if there is not remainder)
            if above: # need to write data with below part cut out again, but with changed key
                # TODO: Record the hole with reference to the below-node
                self.store(transaction) # old node
      
        return        



DataNode.update_forward_refs() # Now Node is known, update before it's derived
