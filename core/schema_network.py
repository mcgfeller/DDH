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
from core import dapp_attrs, schemas, principals, keys, errors, versions


class SchemaNetworkClass():

    NodeColors: typing.ClassVar[dict[str, str]] = {
        'schema': 'blue',
        'schema_range': 'mediumpurple',
        'schema_version': 'violet',
        'sub_schema': 'slateblue',
        'dapp': 'red',
    }

    def __init__(self):
        self._network = networkx.DiGraph()
        self.valid = utils.Invalidatable(self.complete_graph)

    def add_dapp(self, attrs: dapp_attrs.DApp):
        self._network.add_node(attrs, id=attrs.id, type='dapp',
                               cost=attrs.estimated_cost(), availability_user_dependent=attrs.availability_user_dependent())

    def add_schema(self, key: keys.DDHkeyGeneric, attrs: schemas.SchemaAttributes):
        """ Add base schema, variant/version from attrs, and references to other schemas as subpath -> other_range """
        assert key == key.without_variant_version()
        key = key.ens()
        # specific vv:
        assert attrs.version != versions.Unspecified, f'schema at {key} has unspecified version'
        vvkey = keys.DDHkeyVersioned(key.key, fork=keys.ForkType.schema, variant=attrs.variant, version=attrs.version)
        # base node and vv:
        self.add_schema_vv(key, vvkey)
        # add references to other schema:
        for subpath, ref in attrs.references.items():
            # print(f'add_schema reference {vvkey=} {subpath=} -> {ref=}')
            subkey = vvkey+subpath  # the reference is from the versioned subkey
            if subpath:  # if not ==vvkey, add subkey and link it:
                self._network.add_node(subkey, id=str(subkey), type='sub_schema')
                self._network.add_edge(vvkey, subkey, type='sub')
            self.add_schema_reference(subkey, typing.cast(keys.DDHkeyRange, ref.ens()))
        return

    def add_schema_vv(self, key: keys.DDHkeyGeneric, vvkey: keys.DDHkeyVersioned):
        """ Add a concrete variant/version, its base schema, and a version link between them. """
        self._network.add_node(key, id=str(key), type='schema')
        self._network.add_node(vvkey, id=str(vvkey), type='schema_version')
        # Add edge between base and vv!
        self._network.add_edge(key, vvkey, type='version')
        return

    def add_schema_reference(self, vvkey: keys.DDHkeyVersioned, ref: keys.DDHkeyRange):
        """ A reference from vvkey to a reference range """
        self.add_schema_range(ref)
        # Add edge between own vv and ref range:
        self._network.add_edge(vvkey, ref, type='references')
        return

    def add_schema_range(self, rangekey: keys.DDHkeyRange):
        """ add a range and base schema, link them.
            Note: the link is from base to range, so there is no direct path in the graph 
            .complete_graph() will link the range with applicable versions, creating a path.
        """
        refbase = rangekey.without_variant_version()
        self._network.add_node(refbase, id=str(refbase), type='schema')  # ensure base of reference is there
        self._network.add_node(rangekey, id=str(rangekey), type='schema_range')  # reference to range
        self._network.add_edge(refbase, rangekey, type='range')

    def add_edge(self, attrs, target, type, weight=None):
        """ Add link (by outside caller); used to add DApp depends/provides """
        self._network.add_edge(attrs, target, type=type, weight=weight)
        return

    def plot(self, stream, layout='circular_layout', size_h=1200, center_schema: keys.DDHbaseModel | None = None, radius: int = 2):
        """ Return a graph on stream of the current network.
            If center_schema is given, it is used as the center of an ego_graph.
        """
        self.valid.use()

        flayout = getattr(networkx, layout, None)
        if not flayout:
            raise errors.NotAcceptable('Layout not available').to_http()

        plt.clf()  # reset any existing figure
        # size is given in px, calculate in inch
        size_h = size_h // 100; size_v = size_h * 9//10
        size_f = size_h // 2  # font size in pt
        fig = plt.figure(None, figsize=(size_h, size_v), dpi=100)
        G = self._network
        if center_schema:
            if center_schema not in G.nodes:
                raise errors.NotFound('Center key schema not found').to_http()
            G = networkx.ego_graph(G, center_schema, radius=radius, undirected=True)
        # G = networkx.ego_graph(G, keys.DDHkey('//p/employment/salary/statements'), radius=3, undirected=True)
        try:
            pos = flayout(G)
        except Exception as e:
            raise errors.DDHerror(f'Layouting error: {e}; choose another layout').to_http()

        labels = {node: f"{attrs.get('id','-unknown-')}" for node, attrs in G.nodes.items()}  # short id for nodes
        colors = [self.NodeColors.get(attrs.get('type', '-unknown-'), 'black') for attrs in G.nodes.values()]
        networkx.draw_networkx(G, pos=pos, with_labels=True,
                               labels=labels, node_color=colors, font_size=size_f)
        networkx.draw_networkx_edge_labels(
            G, pos, edge_labels=networkx.get_edge_attributes(G, 'type'))
        plt.savefig(stream, format='png', bbox_inches='tight')  # bbox_inches -> no frame
        stream.seek(0)  # rewind
        return

    def dapps_from(self, from_dapp: dapp_attrs.DApp, principal: principals.Principal) -> typing.Iterable[dapp_attrs.DApp]:
        self.valid.use()
        return [n for n in networkx.ancestors(self._network, from_dapp) if isinstance(n, dapp_attrs.DApp)]

    def dapps_required(self, for_dapp: dapp_attrs.DApp, principal: principals.Principal) -> tuple[set[dapp_attrs.DApp], set[dapp_attrs.DApp]]:
        """ return two sets of DApps required by this DApp
            -   all required 
            -   required for cost calculation, considering despite schemas.Requires annotations, for which
                we take only the longest line of requires DApps as a conservative estimate.

        """
        self.valid.use()
        g = self._network
        # sp = networkx.shortest_path(g, target=for_dapp)
        # lines = [{x for x in l if isinstance(x, dapp_attrs.DApp)} for l in sp.keys()]
        # suggested = set.union(*lines)
        preds = networkx.predecessor(g, for_dapp)
        suggested = {s for s in preds.keys() if isinstance(s, dapp_attrs.DApp)}  # only DApps
        return suggested, suggested

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

            Compile all schemas and their variant/versions (vv) that are provided by a DApp.

            For each range, check the corresponding schema for matching vv, and add 'fulfills' edges
            from the range to the vv. Go up the range's schema to find 

        """
        # all schemas
        schemas = {node for node, typ in self._network.nodes(data='type', default=None) if typ == 'schema'}
        # {schema : {variant_version}} for all schemas, where variant_version has a 'provided by' edge; i.e., is provided
        # by a DApp apart from being just a schema version.
        # Consider: For graph purposes, this is actually not needed, as the traversal goes to the DApp; however,
        # omitting it could clutter the graph.
        vv_by_schema = {schema: vvs for schema in schemas if (
            # set of successors of schema of type version and at least one out-edge of type 'provided by':
            vvs := {k for k, v in self._network.succ[schema].items() if v['type'] == 'version' and
                    bool([e for x, e, typ in self._network.out_edges(k, data='type', default=None) if typ == 'provided by'])})}

        # print(f'complete_graph {vv_by_schema=}')

        # all ranges we need to treat:
        ranges = {node for node, typ in self._network.nodes(data='type', default=None) if typ == 'schema_range'}
        for srange in ranges:
            # get the base schema for this range - type is type of edge:
            schema = [k for k, v in self._network.pred[srange].items() if v['type'] == 'range']
            if len(schema) == 1:
                schema = schema[0]
            else:
                raise ValueError(f'Network range {srange} has no unique schema')

            while schema:  # exhausted?
                if (svvs := vv_by_schema.get(schema)):
                    # Add an edge from the range to all version with equal variants and fulfilling the version constraint:
                    svvs_match = [svv for svv in svvs if svv in srange]
                    # print(f'range {srange!r} against candidates {svvs} \n-> match {svvs_match}')
                    [self._network.add_edge(srange, svv, type='fulfills') for svv in svvs_match]

                schema = schema.up()  # go path up until exhausted
        return
