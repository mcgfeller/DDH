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


    def __repr__(self):
        """ Normal display is excessively long """
        return f'{self.__class__.__name__}(id={self.id})'

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

CostToWeight = {
    EstimatedCosts.free: 0.0,
    EstimatedCosts.low: 1.0,
    EstimatedCosts.medium: 3.0,
    EstimatedCosts.high: 5.0,
}


class DApp(DAppOrFamily):
    class Config:
        extra = 'allow' # DApps are free to use their own variables
    
    belongsTo: typing.Optional[DAppFamily] = None
    references : list[relationships.Reference] = []
    estimatedCosts : EstimatedCosts = EstimatedCosts.free


    def __init__(self,*a,**kw):
        """ Add to family as member """
        super().__init__(*a,**kw)
        if self.belongsTo:
            self.belongsTo.members[self.id] = self


    def __hash__(self):
        return hash(self.id)

    def __eq__(self,other):
        return (self.id == other.id) if isinstance(other,DApp) else False
            

    def get_schemas(self) -> dict[keys.DDHkey,schemas.AbstractSchema]:
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

    def estimated_cost(self) -> float:
        """ return cost estimate or EstimatedCosts.user if it is user-dependent (e.g., memberships) """
        if self.estimatedCosts == EstimatedCosts.user:
            cost = 1.0
        else:
            cost = CostToWeight[self.estimatedCosts]
        return cost

    def cost_for_user(self,principal: principals.Principal) -> float:
        """ return cost of this DApp for a user, for selection purposes only.
        """
        return 0.0

    @classmethod
    def bootstrap(cls,session,pillars : dict) -> typing.Union[DApp,tuple[DApp]]:
        return cls()

    def startup(self,session,pillars : dict)  -> list[nodes.Node]:
        schemaNetwork : pillars.SchemaNetworkClass = pillars['SchemaNetwork']
        dnodes = self.register_schema(session)
        self.register_references(session,schemaNetwork)

        return dnodes

    def register_references(self,session, schemaNetwork : pillars.SchemaNetworkClass):
        transaction = session.get_or_create_transaction()
        schemaNetwork.network.add_node(self,id=self.id,type='dapp',
            cost=self.estimated_cost(),availability_user_dependent=self.availability_user_dependent())
        for ref in self.get_references():
            # we want node attributes of, so get the node: 
            snode,split = keydirectory.NodeRegistry.get_node(ref.target,nodes.NodeSupports.schema,transaction) # get applicable schema node for attributes
            sa = snode.nschema.schema_attributes
            schemaNetwork.network.add_node(ref.target,id=str(ref.target),type='schema',requires=sa.requires)
            if ref.relation == relationships.Relation.provides:
                schemaNetwork.network.add_edge(self,ref.target,type='provides',weight=self.get_weight())
            elif ref.relation == relationships.Relation.requires:
                schemaNetwork.network.add_edge(ref.target,self,type='requires')
        return

    def get_weight(self) -> float:
        """ get weight based on costs """
        return 1.0 + self.estimated_cost()


    def register_schema(self,session) -> list[nodes.Node]:
        transaction = session.get_or_create_transaction()
        
        dnodes = []
        for schemakey,schema in self.get_schemas().items():
            dnode = keydirectory.NodeRegistry[schemakey].get(nodes.NodeSupports.schema) # need exact location, not up the tree
            if dnode:
                dnode = dnode.ensure_loaded(transaction)
            else:
                # create dnode with our schema:
                dnode = DAppNode(owner=self.owner,schema=schema,dapp=self,consents=schemas.AbstractSchema.get_schema_consents())
                keydirectory.NodeRegistry[schemakey] = dnode

                # hook into parent schema:
                schemas.AbstractSchema.insert_schema(self.id, schemakey,transaction)
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
 
