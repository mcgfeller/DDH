""" Support for DApps """
from __future__ import annotations
from abc import abstractmethod
import typing

from core import keys,permissions,schemas,nodes,keydirectory,policies,errors,transactions,principals
from utils.pydantic_utils import NoCopyBaseModel



class DApp(NoCopyBaseModel):
    
    owner : typing.ClassVar[principals.Principal] 
    schemakey : typing.ClassVar[keys.DDHkey] 
    policy: policies.Policy = policies.EmptyPolicy
    

    @property
    def id(self) -> principals.DAppId:
        """ Default DAppId is class name """
        return typing.cast(principals.DAppId,self.__class__.__name__) 

    @classmethod
    def bootstrap(cls,session) -> DApp:
        return cls()

    def startup(self,session)  -> nodes.Node:
        dnode = self.check_registry(session)
        return dnode

    def check_registry(self,session) -> nodes.Node:
        transaction = session.get_or_create_transaction()
        dnode = keydirectory.NodeRegistry[self.schemakey].get(nodes.NodeSupports.schema) # need exact location, not up the tree
        if dnode:
            dnode = dnode.ensure_loaded(transaction)
        else:
            # get a parent scheme to hook into
            upnode,split = keydirectory.NodeRegistry.get_node(self.schemakey,nodes.NodeSupports.schema,transaction)
            pkey = self.schemakey.up()
            if not pkey:
                raise ValueError(f'{self.schemakey} key is too high {self!r}')
            upnode = typing.cast(nodes.SchemaNode,upnode)
            parent = upnode.get_sub_schema(pkey,split)
            if not parent:
                raise ValueError(f'No parent schema found for {self!r} with {self.schemakey} at upnode {upnode}')
            schema = self.get_schema() # obtain static schema
            # give world schema_read access
            consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[principals.AllPrincipal],withModes={permissions.AccessMode.schema_read})])
            dnode = DAppNode(owner=self.owner,schema=schema,dapp=self,consents=consents)
            keydirectory.NodeRegistry[self.schemakey] = dnode
            # now insert our schema into the parent's:
            schemaref = schemas.SchemaReference.create_from_key(self.__class__.__name__,ddhkey=self.schemakey)
            parent.add_fields({self.schemakey[-1] : (schemaref,None)})
        return dnode 
    
    def get_schema(self) -> schemas.Schema:
        """ Obtain initial schema for DApp - this is stored in the Node and must be static. """
        raise errors.SubClass

    @abstractmethod
    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        return  data
    


class DAppNode(nodes.ExecutableNode):
    """ node managed by a DApp """
    dapp : DApp


    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        r = self.dapp.execute(op,access,transaction, key_split, data, q)
        return r
 
