""" Support for DApps """
from __future__ import annotations
from abc import abstractmethod
import enum
import typing
import pydantic

from core import keys,permissions,schemas,nodes,keydirectory,policies,errors,transactions,principals,relationships,pillars,common_ids
from utils.pydantic_utils import NoCopyBaseModel


class DAppOrFamily(NoCopyBaseModel):
    """ common properties between DApp and DAppFamily """
    class Config:
        extra = 'ignore'

    id : typing.Optional[str]  = None # principals.DAppId causes Pydantic errors - I don't know why
    description : typing.Optional[str] = None
    owner : typing.ClassVar[principals.Principal] 
    policy: policies.Policy = policies.EmptyPolicy
    catalog : common_ids.CatalogCategory 
    dependsOn: set[DAppOrFamily] = set()
    labels : dict[common_ids.Label,typing.Any] = {}

    searchtext : typing.Optional[str] = None

    def __init__(self,*a,**kw):
        """ Calculate labels; would like a computed and cached property,
            but Pydantic currently doesn't support that:
            https://github.com/samuelcolvin/pydantic/pull/2625
        """
        super().__init__(*a,**kw)
        if not self.id:
            self.id = typing.cast(principals.DAppId,self.__class__.__name__) 
        if not self.description:
            self.description = str(self.id)
        if not self.searchtext:
            self.searchtext = self.description.lower()
        else:
            self.searchtext = self.searchtext.lower()
        self.labels = self.compute_labels()


    def compute_labels(self) ->dict:
        """ Compute and assign labels """
        # TODO: Compute labels from other attributes
        return {common_ids.Label.id:self.id}
    
    def to_DAppOrFamily(self):
        """ convert DApp or DAppFamily to DAppOrFamily, which is the FastAPI ResponseModel  """
        return DAppOrFamily(**self.dict()) # excess attributes are ignore

DAppOrFamily.update_forward_refs()

class DAppFamily(DAppOrFamily):
    """ A DAppFamily is a collection of DApps that can be subscribed together """
    members : dict[principals.DAppId,DApp] = {}


@enum.unique
class EstimatedCosts(str,enum.Enum):
    """ Operations """

    free = 'free'
    low = 'low'
    medium = 'medium'
    high = 'high'
    user = 'user dependent'    


class DApp(DAppOrFamily):
    class Config:
        extra = 'allow' # DApps are free to use their own variables
    
    belongsTo: typing.Optional[DAppFamily] = None
    references : list[relationships.Reference] = []


    def __init__(self,*a,**kw):
        """ Add to family as member """
        super().__init__(*a,**kw)
        if self.belongsTo:
            self.belongsTo.members[self.id] = self


    def __hash__(self):
        return hash(self.id)

    def __eq__(self,other):
        return (self.id == other.id) if isinstance(other,DApp) else False
            

    def get_schemas(self) -> dict[keys.DDHkey,schemas.Schema]:
        """ Obtain initial schema for DApp """
        return {}


   
    def get_references(self):
        """ return references; can be overwritten """
        return self.references

    def add_reference(self,references: list[relationships.Reference]) -> DApp:
        """ add a reference and returns self for chaining """
        self.references.extend(references)
        return self

    def availability_user_dependent(self) -> bool:
        """ is the availability dependent on the user, e.g., for employee DApps.
            the concrete availability can be determined by .availability_for_user()
        """
        return False 

    def availability_for_user(self,principal: principals.Principal) -> bool:
        """ Whether this DApp can be obtained by this user, for selection purposes only.
        """
        return True

    def estimated_cost(self) -> EstimatedCosts:
        """ retrun cost estimate or EstimatedCosts.user if it is user-dependent (e.g., memberships) """
        return EstimatedCosts.free

    def cost_for_user(self,principal: principals.Principal) -> float:
        """ return cost of this DApp for a user, for selection purposes only.
        """
        return 0.0

    @classmethod
    def bootstrap(cls,session,pillars : dict) -> typing.Union[DApp,tuple[DApp]]:
        return cls()

    def startup(self,session,pillars : dict)  -> list[nodes.Node]:
        schemaNetwork : pillars.SchemaNetworkClass = pillars['SchemaNetwork']
        self.register_references(schemaNetwork)
        dnodes = self.register_schema(session)

        return dnodes

    def register_references(self,schemaNetwork : pillars.SchemaNetworkClass):
        schemaNetwork.network.add_node(self,id=self.id,type='dapp',
            cost=self.estimated_cost(),availability_user_dependent=self.availability_user_dependent())
        for ref in self.get_references():
            schemaNetwork.network.add_node(ref.target,id=str(ref.target),type='schema')
            if ref.relation == relationships.Relation.provides:
                schemaNetwork.network.add_edge(self,ref.target,type='provides')
            elif ref.relation == relationships.Relation.requires:
                schemaNetwork.network.add_edge(ref.target,self,type='requires')
        return

    def register_schema(self,session) -> list[nodes.Node]:
        transaction = session.get_or_create_transaction()
        
        dnodes = []
        for schemakey,schema in self.get_schemas().items():
            dnode = keydirectory.NodeRegistry[schemakey].get(nodes.NodeSupports.schema) # need exact location, not up the tree
            if dnode:
                dnode = dnode.ensure_loaded(transaction)
            else:
                # get a parent scheme to hook into
                upnode,split = keydirectory.NodeRegistry.get_node(schemakey,nodes.NodeSupports.schema,transaction)
                pkey = schemakey.up()
                if not pkey:
                    raise ValueError(f'{schemakey} key is too high {self!r}')
                upnode = typing.cast(nodes.SchemaNode,upnode)
                parent = upnode.get_sub_schema(pkey,split)
                if not parent:
                    raise ValueError(f'No parent schema found for {self!r} with {schemakey} at upnode {upnode}')
                # give world schema_read access
                consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[principals.AllPrincipal],withModes={permissions.AccessMode.schema_read})])
                dnode = DAppNode(owner=self.owner,schema=schema,dapp=self,consents=consents)
                keydirectory.NodeRegistry[schemakey] = dnode
                # now insert our schema into the parent's:
                assert self.id
                schemaref = schemas.SchemaReference.create_from_key(self.id,ddhkey=schemakey)
                parent.add_fields({schemakey[-1] : (schemaref,None)})
            dnodes.append(dnode)
        return dnodes 
    

    @abstractmethod
    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        return  data
    


class DAppNode(nodes.ExecutableNode):
    """ node managed by a DApp """
    dapp : DApp

    def __hash__(self):
        return hash(self.dapp)

    def __eq__(self,other):
        return (self.dapp == other.dapp) if isinstance(other,DAppNode) else False



    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        r = self.dapp.execute(op,access,transaction, key_split, data, q)
        return r
 
