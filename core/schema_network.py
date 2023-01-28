""" The Network of AbstractSchema Relationsships """
from __future__ import annotations
import typing
import logging

import networkx
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use('Agg')  # Server-side rendering


logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_attrs, schemas, principals, keys, errors


class SchemaNetworkClass():

    def __init__(self):
        self._network = networkx.DiGraph()
        self.valid = utils.Invalidatable(self.complete_graph)

    def add_dapp(self, attrs: dapp_attrs.DApp):
        self._network.add_node(attrs, id=attrs.id, type='dapp',
                               cost=attrs.estimated_cost(), availability_user_dependent=attrs.availability_user_dependent())

    def add_schema(self, key: keys.DDHkey, attrs: schemas.SchemaAttributes):
        assert key is key.without_variant_version()
        # base node without vv:
        self._network.add_node(key, id=str(key), type='schema')
        # specific vv:
        vvkey = keys.DDHkey(key.key, fork=keys.ForkType.schema, variant=attrs.variant, version=attrs.version)
        self._network.add_node(vvkey, id=str(vvkey), type='schema')
        # TODO: Add edge between base and vv!
        # add references:
        for ref in attrs.references:
            self.add_schema_reference(vvkey, ref)
        return

    def add_schema_reference(self, vvkey: keys.DDHkey, ref: keys.DDHkey):
        refbase = ref.without_variant_version()
        self._network.add_node(refbase, id=str(refbase), type='schema')  # ensure base of reference is there
        self._network.add_node(ref, id=str(ref), type='schema')  # reference with vv
        # TODO: Add edge between base and vv!
        self._network.add_edge(vvkey, ref, type='references')
        return

    def add_schema_node(self, schema_key: keys.DDHkey, schema_attrs: schemas.SchemaAttributes):
        self._network.add_node(schema_key, id=str(schema_key), type='schema', requires=schema_attrs.requires)

    def add_edge(self, attrs, target, type, weight=None):
        self._network.add_edge(attrs, target, type=type, weight=weight)

    def plot(self, stream, layout='circular_layout', size_h=1200):
        """ Return a graph on stream of the current network """
        self.valid.use()
        labels = {node: f"{attrs['type']}:{attrs['id']}" for node,
                  attrs in self._network.nodes.items()}  # short id for nodes
        colors = ['blue' if attrs['type'] ==
                  'schema' else 'red' for attrs in self._network.nodes.values()]
        flayout = getattr(networkx, layout, None)
        if not flayout:
            raise errors.NotAcceptable('Layout not available').to_http()
        try:
            pos = flayout(self._network)
        except Exception as e:
            raise errors.DDHerror(f'Layouting error: {e}; choose another layout').to_http()
        plt.clf()  # reset any existing figure
        # size is given in px, calculate in inch
        size_h = size_h // 100; size_v = size_h * 9//10
        size_f = size_h // 2  # font size in pt
        fig = plt.figure(None, figsize=(size_h, size_v), dpi=100)
        networkx.draw_networkx(self._network, pos=pos, with_labels=True,
                               labels=labels, node_color=colors, font_size=size_f)
        plt.savefig(stream, format='png')
        stream.seek(0)  # rewind
        return

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
