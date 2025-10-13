
""" DDH Core Node Models for Excecutable and Schema providing Nodes """

from abc import abstractmethod
import typing

from backend import persistable

from core import keys, node_types, nodes, keydirectory, relationships, schema_network, dapp_attrs, schemas as m_schemas, principals
from core.node_types import NodeSupports


class ExecutableNode(node_types.T_ExecutableNode, nodes.Node, persistable.NonPersistable):
    """ A node that provides for execution capabilities """

    @property
    def supports(self) -> set[NodeSupports]:
        s = {NodeSupports.execute}
        if self.consents:
            s.add(NodeSupports.consents)
        return s

    @abstractmethod
    async def execute(self, req: dapp_attrs.ExecuteRequest):
        return {}


class SchemedExecutableNode(ExecutableNode):
    """ Node that is executable and provides a Schema """

    subscribable: bool = False  # is this node providing a subscribable schema?

    @property
    def supports(self) -> set[NodeSupports]:
        """ Note that this is NOT a SchemaNode, and does not support .schema. The schema must be registered
            separately, under  a schema key. 
        """
        s = {NodeSupports.execute, }
        if self.subscribable:
            s.add(NodeSupports.subscribable)
        if self.consents:
            s.add(NodeSupports.consents)
        return s

    def register_references(self, attrs, session, schema_network: schema_network.SchemaNetworkClass):
        schema_network.add_dapp(attrs)

        for ref in attrs.references:
            target = ref.target.ens()
            if ref.relation == relationships.Relation.provides:
                target = keys.DDHkeyVersioned0.cast(target)
                schema_network.add_schema_vv(target.without_variant_version(), target)
                schema_network.add_edge(target, attrs, type='provided by', weight=attrs.get_weight())
                # register our node as a provider for (or transformer into) the key:
                keydirectory.NodeRegistry[ref.target] = self
            elif ref.relation == relationships.Relation.requires:
                target = keys.DDHkeyRange.cast(target)
                schema_network.add_schema_range(target)
                schema_network.add_edge(attrs, target, type='requires')
        schema_network.valid.invalidate()  # we have modified the network

    @staticmethod
    def register_schema(schemakey: keys.DDHkeyVersioned, schema: m_schemas.AbstractSchema, owner: principals.Principal, transaction):
        """ register a single schema in its Schema Node, creating one if necessary. 
            staticmethod so it can be called by test fixtures. 
        """
        genkey = schemakey.without_variant_version()
        snode = keydirectory.NodeRegistry[genkey].get(
            nodes.NodeSupports.schema)  # need exact location, not up the tree
        # hook into parent schema:
        parent, split = m_schemas.AbstractSchema.get_parent_schema(transaction, genkey)
        # inherit transformers:
        schema.schema_attributes.transformers = parent.schema_attributes.transformers.merge(
            schema.schema_attributes.transformers)

        if snode:
            # TODO:#33: Schema Nodes don't need .ensure_loaded now, but should be reinserted once they're async
            snode = typing.cast(nodes.SchemaNode, snode)
            snode.add_schema(schema)
        else:
            # create snode with our schema:
            snode = nodes.SchemaNode(owner=owner, consents=m_schemas.AbstractSchema.get_schema_consents())
            keydirectory.NodeRegistry[genkey] = snode
            snode.add_schema(schema)

            parent.insert_schema_ref(transaction, genkey, split)
        return snode


class InProcessSchemedExecutableNode(SchemedExecutableNode):

    attrs: dapp_attrs.SchemaProvider

    def register(self, session):
        assert self.key
        transaction = session.get_or_create_transaction()
        for k, s in self.get_schemas().items():
            snode = self.register_schema(k, s, self.owner, transaction)
        self.register_references(self.attrs, session, m_schemas.SchemaNetwork)
        keydirectory.NodeRegistry[self.key] = self
        m_schemas.SchemaNetwork.valid.invalidate()  # finished
        return
