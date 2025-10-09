
""" Directory mapping keys to nodes """

from abc import abstractmethod
import pydantic
import datetime
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from . import nodes, keys, transactions
from utils import utils
from backend import persistable


class _NodeRegistry:
    """ Preliminary holder of nodes
        Note that Nodes are held per NodeSupports, duplicating them as required
        for easy lookup by NodeSupports.

        A proper realization could use a PatriciaTrie.

        # TODO: Are nodes on DDHKey or DDHKeyGeneric?
    """

    nodes_by_key: dict[tuple, dict[nodes.NodeSupports, nodes.NodeProxy]]  # by key, then by NodeTypes

    def __init__(self):
        self.nodes_by_key = {}

    def _clear(self, supports: set[nodes.NodeSupports]):
        """ clear selective supports, for testing only """
        for s in self.nodes_by_key:
            if s in supports:
                self.nodes_by_key[s].clear()
        return

    def __setitem__(self, key: keys.DDHkey, node: nodes.NodeOrProxy):
        """ Store the node, with a reference per NodeSupports """
        node.key = key
        proxy = node.get_proxy()
        by_supports = self.nodes_by_key.setdefault(key.key, {})
        for s in proxy.supports:
            by_supports[s] = proxy
        return

    def check_and_set(self, key: keys.DDHkey, node: nodes.NodeOrProxy) -> bool:
        """ Conditional store, do nothing if already stored under some support.
            Return True if newly inserted.
        """
        if (inserted := key not in self.nodes_by_key):
            self.__setitem__(key, node)
        return inserted

    def __getitem__(self, key: keys.DDHkey) -> dict[nodes.NodeSupports, persistable.PersistableProxy]:
        return self.nodes_by_key.get(key.key, {})

    def get_next_proxy(self, key: keys.DDHkey | None, support: nodes.NodeSupports) -> typing.Iterator[typing.Tuple[nodes.NodeOrProxy, int]]:
        """ Generator getting next node walking up the tree from key.
            Also indicates at which point the keys.DDHkey is to be split so the first part is the
            path leading to the Node, the 2nd the rest.
            """
        split = len(key.key)  # where to split: counting backwards from the end.
        while key:
            by_supports = self[key]
            key = key.up()
            split -= 1
            nop = by_supports.get(support)  # required support?
            if nop:
                yield nop, split+1
        else:
            return

    def get_proxy(self, key: keys.DDHkey, support: nodes.NodeSupports) -> typing.Tuple[nodes.NodeOrProxy | None, int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        nop, split = next(((node, split) for node, split in self.get_next_proxy(key, support)), (None, -1))
        return nop, split

    def get_node(self, key: keys.DDHkey, support: nodes.NodeSupports, transaction: transactions.Transaction,
                 condition: typing.Callable | None = None) -> typing.Tuple[nodes.Node | None, int]:
        """ get a node that supports support, walking up the tree.
            ProxyNodes are loaded.
            If the Node doesn't meet condition, the search goes up the tree looking for a Node.


            TODO:#33: Eventually, all calls must be async.
        """
        nop, split = self.get_proxy(key, support)
        if nop:
            node = nop
            assert isinstance(node, nodes.Node)  # searchable Persistable must be Node
            if condition and not condition(node):  # apply condition to loaded Node
                key = key.up()  # go one up and recurse
                if key:
                    node, split = self.get_node(key, support, transaction, condition=condition)
                    split -= 1
                else:
                    return (None, -1)
        else:
            node = None

        return node, split

    async def get_node_async(self, key: keys.DDHkey, support: nodes.NodeSupports, transaction: transactions.Transaction,
                             condition: typing.Callable | None = None) -> typing.Tuple[nodes.Node | None, int]:
        """ get a node that supports support, walking up the tree.
            ProxyNodes are loaded.
            If the Node doesn't meet condition, the search goes up the tree looking for a Node.
        """
        nop, split = self.get_proxy(key, support)
        if nop:
            node = await nop.ensure_loaded(transaction)
            assert isinstance(node, nodes.Node)  # searchable Persistable must be Node
            if condition and not condition(node):  # apply condition to loaded Node
                key = key.up()  # go one up and recurse
                if key:
                    node, split = await self.get_node_async(key, support, transaction, condition=condition)
                    split -= 1
                else:
                    return (None, -1)
        else:
            node = None

        return node, split

    @staticmethod
    def _get_consent_node(ddhkey: keys.DDHkey, support: nodes.NodeSupports, node: nodes.Node | None, transaction: transactions.Transaction) -> nodes.Node | None:
        """ get consents, from current node or from its parent
            TODO:#33: Eventually, all calls must be async.
        """
        if node and node.has_consents():
            cnode = node
        else:
            cnode, d = NodeRegistry.get_node(
                ddhkey, support, transaction, condition=nodes.Node.has_consents)
            if not cnode:  # means that upper nodes don't have consent
                cnode = node
        return cnode

    @staticmethod
    async def _get_consent_node_async(ddhkey: keys.DDHkey, support: nodes.NodeSupports, node: nodes.Node | None, transaction: transactions.Transaction) -> nodes.Node | None:
        """ get consents, from current node or from its parent """
        if node and node.has_consents():
            cnode = node
        else:
            cnode, d = await NodeRegistry.get_node_async(
                ddhkey, support, transaction, condition=nodes.Node.has_consents)
            if not cnode:  # means that upper nodes don't have consent
                cnode = node
        return cnode

    def get_keys_with_prefix(self, prefix: keys.DDHkey) -> tuple[tuple, ...]:
        """ get all keys that match prefix, as a tuple of key tuples; serves as input to get_nodes_from_keys """
        return utils.filter_prefix(prefix.key, self.nodes_by_key.keys())

    async def get_nodes_from_tuple_keys(self, node_keys: typing.Sequence[typing.Sequence], support: nodes.NodeSupports, transaction: transactions.Transaction) -> list[nodes.Node]:
        """ for all keys (as tuples from .get_keys_with_prefix()) that have support, load and return their nodes. """
        return [await node_proxy.ensure_loaded(transaction) for nk in node_keys if (node_proxy := self.nodes_by_key[nk].get(support.value, None))]


NodeRegistry = _NodeRegistry()
