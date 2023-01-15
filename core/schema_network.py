""" The Network of AbstractSchema Relationsships """
from __future__ import annotations
import typing
import logging

import networkx
import matplotlib.pyplot as plt


logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_attrs, schemas, principals, keys


class SchemaNetworkClass():

    def __init__(self):
        self._network = networkx.DiGraph()
        self.valid = utils.Invalidatable(self.complete_graph)

    def add_dapp(self, attrs: dapp_attrs.DApp):
        self._network.add_node(attrs, id=attrs.id, type='dapp',
                               cost=attrs.estimated_cost(), availability_user_dependent=attrs.availability_user_dependent())

    def add_schema(self, attrs: schemas.SchemaAttributes):
        ...

    def add_schema_node(self, target: keys.DDHkey, attrs: schemas.SchemaAttributes):
        self._network.add_node(target, id=str(target), type='schema', requires=attrs.requires)

    def add_edge(self, attrs, target, type, weight=None):
        self._network.add_edge(attrs, target, type=type, weight=weight)

    def plot(self, layout='circular_layout'):
        """ Plot the current network """
        self.valid.use()
        labels = {node: f"{attrs['type']}:{attrs['id']}" for node,
                  attrs in self._network.nodes.items()}  # short id for nodes
        colors = ['blue' if attrs['type'] ==
                  'schema' else 'red' for attrs in self._network.nodes.values()]
        flayout = getattr(networkx, layout)
        networkx.draw_networkx(self._network, pos=flayout(self._network),
                               with_labels=True, labels=labels, node_color=colors)
        plt.show()

    def dapps_from(self, from_dapp: dapp_attrs.DApp, principal: principals.Principal) -> typing.Iterable[dapp_attrs.DApp]:
        self.valid.use()
        return [n for n in networkx.descendants(self._network, from_dapp) if isinstance(n, dapp_attrs.DApp)]

    def dapps_required(self, for_dapp: dapp_attrs.DApp, principal: principals.Principal) -> tuple[set[dapp_attrs.DApp], set[dapp_attrs.DApp]]:
        """ return two sets of DApps required by this DApp
            -   all required 
            -   required for cost calculation, considering despite schemas.Requires annotations, for which
                we take only the longest line of requires DApps as a conservative estimate.

        """
        self.valid.use()
        g = self._network
        sp = networkx.shortest_path(g, target=for_dapp)

        lines = [{x for x in l if isinstance(x, dapp_attrs.DApp)} for l in sp.values()]
        suggested = set.union(*lines)

        # node: req if node has any requirement:
        nodes_with_reqs = {k: req for k in sp.keys() if (
            nk := g.nodes[k])['type'] == 'schema' and (req := nk.get('requires'))}
        if nodes_with_reqs:
            discard = set()  # lines to be discarded, cumulative for all nodes
            for node in nodes_with_reqs:
                req = nodes_with_reqs[node]
                # use longest line unless .all
                if req in (schemas.Requires.one, schemas.Requires.specific, schemas.Requires.few):
                    # len, line if attributed schema is in the path:
                    ll = [(len(v), i) for i, (k, v) in enumerate(sp.items()) if node in v]
                    retain = max(ll)  # longest line (higher index for ties)
                    discard.update({d[1] for d in ll if retain != d})  # others are to be discarded
            # all lines except those discarded
            lines = [l for i, l in enumerate(lines) if i not in discard]
            calculated = set.union(*lines)
        else:  # no attributes, we use all nodes
            calculated = suggested
        return suggested, calculated

    def complete_graph(self):
        """ Finish up the graph after all nodes have been added:

            Keys are provided if key above it is provided.
            For all keys that are requires, 
            check if a parent key is provided, and add a provision from this key to that parent

        """
        # schema nodes that have an out_edge of type 'requires':
        required_nodes = {n for n, n_out, t in self._network.out_edges((node for node, typ in self._network.nodes(
            data='type', default=None) if typ == 'schema'), data='type') if t == 'requires'}  # type:ignore

        # schema nodes that have in in_edge of type requires
        provided_nodes = {n for n_in, n, t in self._network.in_edges((node for node, typ in self._network.nodes(
            data='type', default=None) if typ == 'schema'), data='type') if t == 'provides'}  # type:ignore

        for node in required_nodes:
            up = node
            while up := up.up():  # go path up until exhausted
                if up in provided_nodes:  # provided at this level?
                    # extend provision from up level to node
                    self._network.add_edge(up, node, type='provides')

        return
