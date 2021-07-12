
""" DDH Core Node Models """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import datetime
import typing
import enum
import abc
import secrets

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import permissions
from . import schemas



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
    def execute(self, access : permissions.Access, key_split : int, q : typing.Optional[str] = None):
        return {}


class DelegatedExecutableNode(ExecutableNode):
    """ A node that delegates executable methods to DApps """

    executors : list = []

    def execute(self, access : permissions.Access, key_split: int, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        d = None
        for executor in self.executors:
            d = executor.get_and_transform(access,key_split, q)
        return d


