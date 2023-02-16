""" DApps definition, both for Python based DApps and for runner.  """
from __future__ import annotations
from abc import abstractmethod
import enum
import typing
import pydantic

from core import keys, schemas, policies, principals, relationships, common_ids, versions, permissions, transactions, privileges
from utils.pydantic_utils import DDHbaseModel


class DAppOrFamily(DDHbaseModel):
    """ common properties between DApp and DAppFamily """
    class Config:
        extra = 'ignore'

    id: str | None = None  # principals.DAppId causes Pydantic errors - I don't know why
    description: str | None = None
    owner: typing.ClassVar[principals.Principal]
    policy: policies.Policy = policies.EmptyPolicy
    catalog: common_ids.CatalogCategory
    dependsOn: set[DAppOrFamily] = set()
    labels: dict[common_ids.Label, typing.Any] = {}

    searchtext: str | None = None

    def __init__(self, *a, **kw):
        """ Calculate labels; would like a computed and cached property,
            but Pydantic currently doesn't support that:
            https://github.com/samuelcolvin/pydantic/pull/2625
        """
        super().__init__(*a, **kw)
        if not self.id:
            self.id = typing.cast(principals.DAppId, self.__class__.__name__)
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

    def compute_labels(self) -> dict:
        """ Compute and assign labels """
        # TODO: Compute labels from other attributes
        return {common_ids.Label.id: self.id}

    def to_DAppOrFamily(self):
        """ convert DApp or DAppFamily to DAppOrFamily, which is the FastAPI ResponseModel  """
        return DAppOrFamily(**self.dict())  # excess attributes are ignore


DAppOrFamily.update_forward_refs()


class DAppFamily(DAppOrFamily):
    """ A DAppFamily is a collection of DApps that can be subscribed together """
    members: dict[principals.DAppId, DApp] = {}


@enum.unique
class EstimatedCosts(str, enum.Enum):
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
        extra = 'allow'  # DApps are free to use their own variables

    belongsTo: DAppFamily | None = None
    references: list[relationships.Reference] = []
    transforms_into: keys.DDHkeyVersioned0 | None = None  # Versioned0 here to avoid errors for incomplete DApps
    estimatedCosts: EstimatedCosts = EstimatedCosts.free
    requested_privileges: set[privileges.DAppPrivileges] = set()
    granted_privileges: set[privileges.DAppPrivileges] = pydantic.Field(
        default=set(), const=True, description="privileges actually granted, cannot be set")

    def __init__(self, *a, **kw):
        """ Add to family as member """
        super().__init__(*a, **kw)
        if self.belongsTo:
            self.belongsTo.members[self.id] = self
        self.granted_privileges = self.requested_privileges

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return (self.id == other.id) if isinstance(other, DApp) else False

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {}

    def get_references(self):
        """ return references; can be overwritten """
        return self.references

    def add_reference(self, references: list[relationships.Reference]) -> DApp:
        """ add a reference and returns self for chaining """
        self.references.extend(references)
        return self

    def availability_user_dependent(self) -> bool:
        """ is the availability dependent on the user, e.g., for employee DApps.
            the concrete availability can be determined by .availability_for_user()
        """
        return False

    def availability_for_user(self, principal: principals.Principal) -> bool:
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

    def cost_for_user(self, principal: principals.Principal) -> float:
        """ return cost of this DApp for a user, for selection purposes only.
        """
        return 0.0

    def get_weight(self) -> float:
        """ get weight based on costs """
        return 1.0 + self.estimated_cost()


DApp.update_forward_refs()


class RunningDApp(DDHbaseModel):
    id: str | None = None  # principals.DAppId causes Pydantic errors - I don't know why
    dapp_version: versions.Version
    schema_version: versions.Version
    location: pydantic.AnyHttpUrl


class ExecuteRequest(DDHbaseModel):
    """ This is the execution request passed between micro services """
    op: typing.Any  # nodes.Ops
    access: permissions.Access
    transaction: transactions.Transaction
    key_split: int | None = None
    data: dict | None = None
    q: str | None = None
