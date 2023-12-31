
""" DDH Core Node Models """
from __future__ import annotations
from abc import abstractmethod
import pydantic
import typing
import enum


from core import permissions, schemas,  principals, common_ids
from backend import persistable


@enum.unique
class NodeSupports(str, enum.Enum):
    """ Node supports protocol """

    schema = 'schema'
    data = 'data'
    execute = 'execute'
    consents = 'consents'

    def __repr__(self): return self.value


@enum.unique
class Ops(str, enum.Enum):
    """ Operations """

    get = 'get'
    post = 'post'
    put = 'put'
    delete = 'delete'

    def __repr__(self): return self.value


class NodeProxy(persistable.PersistableProxy):
    supports: set[NodeSupports]
    owner_id: common_ids.PrincipalId


class Node(pydantic.BaseModel):

    owner: principals.Principal
    consents: permissions.Consents | None = permissions.DefaultConsents
    key: keys.DDHkey | None = None

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
    def owners(self) -> tuple[principals.Principal, ...]:
        """ get one or multiple owners """
        return (self.owner,)

    def get_proxy(self) -> Node | NodeProxy:
        """ get a loadable proxy for us; idempotent. Reverse .ensureLoaded() """
        if isinstance(self, persistable.Persistable):
            return NodeProxy(supports=self.supports, id=self.id, classname=self.__class__.__name__, owner_id=self.owner.id)
        else:
            return self


from . import keys  # avoid circle
Node.model_rebuild()  # Now Node is known, update before it's derived
NodeOrProxy = Node | persistable.PersistableProxy


class MultiOwnerNode(Node):

    all_owners: tuple[principals.Principal, ...]
    consents: permissions.Consents | permissions.MultiOwnerConsents = permissions.DefaultConsents

    def __init__(self, **data):
        data['owner'] = data.get('all_owners', (None,))[0]  # first owner, will complain in super
        super().__init__(**data)
        # Convert Consents into MultiOwnerConsents:
        if isinstance(self.consents, permissions.Consents):
            self.consents = permissions.MultiOwnerConsents(
                consents_by_owner={self.owner: self.consents})
        elif self.consents:  # sanity check, Consents owners must be node owners
            d = set(self.consents.consents_by_owner.keys())-set(self.all_owners)
            if d:
                raise ValueError(f'Following Consent owners must be Node owners: {d}')
        return

    @property
    def owners(self) -> tuple[principals.Principal, ...]:
        """ get one or multiple owners """
        return self.all_owners


class SchemaNode(Node, persistable.NonPersistable):

    container: schemas.SchemaContainer = schemas.SchemaContainer()
    key: keys.DDHkeyGeneric | None = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

    def add_schema(self, schema: schemas.AbstractSchema):
        assert self.key, 'add schema node to keydirectory.NodeRegistry first'
        self.container.add(self.key, schema)
        return

    @property
    def supports(self) -> set[NodeSupports]:
        s = {NodeSupports.schema}
        if self.consents:
            s.add(NodeSupports.consents)
        return s


from core import dapp_attrs


class ExecutableNode(Node, persistable.NonPersistable):
    """ A node that provides for execution capabilities """

    @property
    def supports(self) -> set[NodeSupports]:
        s = {NodeSupports.execute}
        if self.consents:
            s.add(NodeSupports.consents)
        return s

    @abstractmethod
    def execute(self, req: dapp_attrs.ExecuteRequest):
        return {}
