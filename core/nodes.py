
""" DDH Core Node Models """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import typing
import enum




from . import permissions,schemas,transactions,errors,keydirectory
from utils import datautils
from backend import persistable




@enum.unique
class NodeSupports(str,enum.Enum):
    """ Node supports protocol """

    schema      = 'schema'
    data        = 'data'
    execute     = 'execute'
    consents    = 'consents'

    def __repr__(self): return self.value

@enum.unique
class Ops(str,enum.Enum):
    """ Operations """

    get     = 'get'
    post    = 'post'
    put     = 'put'
    delete  = 'delete'


    def __repr__(self): return self.value

class NodeProxy(persistable.PersistableProxy):
    supports : set[NodeSupports]    



class Node(pydantic.BaseModel):

    owner: permissions.Principal
    consents : typing.Optional[permissions.Consents] = permissions.DefaultConsents
    key : typing.Optional[keys.DDHkey] = None

    @property
    def supports(self) -> set[NodeSupports]:
        return set()

    def has_consents(self):
        """ None consents means to check parent node """
        return self.consents is not None

    def __str__(self):
        """ short representation """
        return f'{self.__class__.__name__}(supports={self.supports},key={self.key!s},owner={self.owner.id})'


    @property
    def owners(self) -> tuple[permissions.Principal,...]:
        """ get one or multiple owners """
        return (self.owner,)

    def get_proxy(self) -> typing.Union[Node,NodeProxy]:
        """ get a loadable proxy for us; idempotent. Reverse .ensureLoaded() """
        if isinstance(self,persistable.Persistable):
            return NodeProxy(supports=self.supports,id=self.id,classname=self.__class__.__name__)
        else:
            return self



from . import keys # avoid circle
Node.update_forward_refs() # Now Node is known, update before it's derived
NodeOrProxy = typing.Union[Node,persistable.PersistableProxy]

class MultiOwnerNode(Node):

    all_owners : tuple[permissions.Principal,...]
    consents : typing.Union[permissions.Consents,permissions.MultiOwnerConsents] = permissions.DefaultConsents

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


class SchemaNode(Node,persistable.NonPersistable):

    nschema : typing.Optional[schemas.Schema] =  pydantic.Field(alias='schema')



    @property
    def supports(self) -> set[NodeSupports]:
        s =  {NodeSupports.schema}
        if self.consents:
            s.add(NodeSupports.consents)
        return s


    def get_sub_schema(self, ddhkey: keys.DDHkey,split: int, schema_type : str = 'json') -> typing.Optional[schemas.Schema]:
        """ return schema based on ddhkey and split """
        s = typing.cast(schemas.Schema,self.nschema)
        s = s.obtain(ddhkey,split)
        return s

class ExecutableNode(SchemaNode):
    """ A node that provides for execution capabilities """

    @property
    def supports(self) -> set[NodeSupports]:
        s =  {NodeSupports.execute}
        if self.nschema:
            s.add(NodeSupports.schema)
        if self.consents:
            s.add(NodeSupports.consents)
        return s


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






class DataNode(Node,persistable.Persistable):
    """ New data node, points to storage and consents """

    owner: permissions.Principal

    format : persistable.DataFormat = persistable.DataFormat.dict
    data : typing.Any        
    storage_loc : typing.Optional[persistable.PersistId] = None
    access_key: typing.Optional[keyvault.AccessKey] = None
    sub_nodes : dict[keys.DDHkey,keys.DDHkey] = {}

    @property
    def supports(self) -> set[NodeSupports]:
        s =  {NodeSupports.data}
        if self.consents:
            s.add(NodeSupports.consents)
        return s



    def store(self, transaction: transactions.Transaction ):
        d = self.to_compressed()
        if self.id not in storage.Storage:
            keyvault.set_new_storage_key(self,transaction.for_user,set(),set())
        enc = keyvault.encrypt_data(transaction.for_user,self,d)
        storage.Storage.store(self.id,enc, transaction)
        return

    @classmethod
    def load(cls, id: persistable.PersistId,  transaction: transactions.Transaction ):
        enc = storage.Storage.load(id,transaction)
        plain = keyvault.decrypt_data(transaction.for_user,self,enc)
        o = cls.from_compressed(plain)
        return o




    def execute(self, op: Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        if key_split:
            top,remainder = access.ddhkey.split_at(key_split)
            if self.format != persistable.DataFormat.dict:
                raise errors.NotSelectable(remainder)
        if op == Ops.get:
            data = self.unsplit_data(self.data,transaction)
            if key_split:
                data = datautils.extract_data(self.data,remainder,raise_error=errors.NotFound)
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
            added = effective = consents.consentees() ; removed = set()

        if added or removed: # expensive op follows, do only if something has changed
            self.consents = consents # actually update
            
            if remainder.key: # change is not at this level, insert a new node:
                node = self.split_node(remainder,consents)
            else:
                above =  None
                node = self # top level

                
            keyvault.set_new_storage_key(node,access.principal,effective,removed) # now we can set the new key

            node.store(transaction) # re-encrypt on new node (may be self if there is not remainder)
            if remainder.key: # need to write data with below part cut out again, but with changed key
                
                self.store(transaction) # old node
      
        return        

    def split_node(self, remainder: keys.DDHkey, consents: permissions.Consents) -> DataNode:
        prev_data = self.data  # need before new key is generated          
        if self.format != persistable.DataFormat.dict:
            raise errors.NotSelectable(remainder)
        key=keys.DDHkey(key=self.key.key+remainder.key)
        above,below = datautils.split_data(prev_data,remainder,raise_error=errors.NotFound) # if we're deep in data
        node = self.__class__(owner=self.owner,key=key,consents=consents,data=below)
        self.data = above
        # Record the hole with reference to the below-node. We keep the below node in the directory, so
        # its data can be accessed without access to self.
        keydirectory.NodeRegistry[key] = node
        return node

    def unsplit_data(self,data,transaction):
        for remainder,fullkey in self.sub_nodes.items():
            subnodeproxy = keydirectory.NodeRegistry[fullkey][NodeSupports.data]
            if subnodeproxy:
                subnode = subnodeproxy.ensure_loaded(transaction)
                assert isinstance(subnode,DataNode) # because of lookup by type
                data = datautils.insert_data(data,remainder,subnode.data)

        return data



DataNode.update_forward_refs() # Now Node is known, update before it's derived
