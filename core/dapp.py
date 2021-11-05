""" Support for DApps """
from __future__ import annotations
from abc import abstractmethod
import typing
import pydantic

from core import keys,permissions,schemas,nodes,keydirectory,policies,errors,transactions,principals
from utils.pydantic_utils import NoCopyBaseModel



class DAppOrFamily(NoCopyBaseModel):
    """ common properties between DApp and DAppFamily """
    class Config:
        extra = 'ignore'

    id : typing.Optional[str]  = None # principals.DAppId causes Pydantic errors!
    owner : typing.ClassVar[principals.Principal] 
    policy: policies.Policy = policies.EmptyPolicy
    dependsOn: set[DAppOrFamily] = set()
    labels : dict[str,typing.Any] = {}
    searchtext : typing.Optional[str] = None

    def __init__(self,*a,**kw):
        """ Calculate labels; would like a computed and cached property,
            but Pydantic currently doesn't support that:
            https://github.com/samuelcolvin/pydantic/pull/2625
        """
        super().__init__(*a,**kw)
        if not self.id:
            self.id = typing.cast(principals.DAppId,self.__class__.__name__) 
        self.labels = self.compute_labels()


    def compute_labels(self) ->dict:
        """ Compute and assign labels """
        # TODO: Compute labels from other attributes
        return {'id':self.id}
    
    def to_DAppOrFamily(self):
        """ convert DApp or DAppFamily to DAppOrFamily """
        return DAppOrFamily(**self.dict())

DAppOrFamily.update_forward_refs()

class DAppFamily(DAppOrFamily):
    members : dict[principals.DAppId,DApp] = {}


class DApp(DAppOrFamily):
    

    schemakey : typing.ClassVar[keys.DDHkey] 
    belongsTo: typing.Optional[DAppFamily] = None


    def __init__(self,*a,**kw):
        """ Add to family as member """
        super().__init__(*a,**kw)
        if self.belongsTo:
            self.belongsTo.members[self.id] = self
    



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
 
