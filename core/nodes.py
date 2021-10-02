
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


from . import permissions
from . import schemas
from . import transactions

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

NodeId = typing.NewType('NodeId', str)

from backend import keyvault,storage

class Persistable(NoCopyBaseModel):
    """ class that provides methods to get persistent state.
        Works with storage.
    """

    id : NodeId = pydantic.Field(default_factory=secrets.token_urlsafe)

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_json(cls, j :str) -> Persistable:
        o = cls.parse_raw(j)
        return o

    def store(self, data: bytes , transaction: transactions.Transaction ):
        storage.Storage.store(self.id,data)
        return

    @classmethod
    def load(cls, id:NodeId, key: bytes,  transaction: transactions.Transaction ):
        data = storage.Storage.load(id,key)
        o = cls.from_json(data.decode())
        return o

    def delete(self, key: bytes, transaction: transactions.Transaction ):
        self.load(self.id, key, transaction) # verify encryption by loading
        storage.Storage.delete(self.id)
        return

    def get_key(self):
        raise NotImplementedError()



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
    def execute(self, access : permissions.Access, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        return {}


class DelegatedExecutableNode(ExecutableNode):
    """ A node that delegates executable methods to DApps """

    executors : list = []

    def execute(self, access : permissions.Access, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
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


    def store(self,access):
        keyvault.set_new_storage_key(self,access.principal,[],[])
        return

    def insert(self,remainder,data):
        """ insert data at remainder key """
        raise NotImplementedError('TODO')
        return

    def read_data(self,principal: permissions.Principal):
        plain = b'data'
        data = keyvault.encrypt_data(principal,self,plain) # TODO: actually read data
        return keyvault.decrypt_data(principal,self,data)

    def write_data(self,principal: permissions.Principal,data):
        data = zlib.compress(data, level=-1)
        enc = keyvault.encrypt_data(principal,self,data)
        self.store(self.id,enc)
        return 

    def update_consents(self,access : permissions.Access, remainder: keys.DDHkey, consents: permissions.Consents):
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
            
            if remainder.key: # change is not at this level, insert a new node:
                key=keys.DDHkey(key=self.key.key+remainder.key)
                node = self.__class__(owner=self.owner,key=key,_consents=consents)
            else:
                node = self # top level

            prev_data = self.read_data(access.principal) # need to read before new key is generated
            keyvault.set_new_storage_key(node,access.principal,effective,removed) # now we can set the new key
            above,below = datautils.splitdata(prev_data,remainder) # if we're deep in data
            node.write_data(access.principal, below) # re-encrypt on new node (may be self if there is not remainder)
            if above: # need to write data with below part cut out again, but with changed key
                self.write_data(access.principal, above) # old node
      
        return        



DataNode.update_forward_refs() # Now Node is known, update before it's derived
