""" The Network of AbstractSchema Relationsships """
from __future__ import annotations
import typing
import logging

import networkx
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_proxy, schema_root,schemas,principals,keys
from utils.pydantic_utils import NoCopyBaseModel



class SchemaNetworkClass():

    def __init__(self):
        self.network = networkx.DiGraph()

    def plot(self,layout='circular_layout'):
        """ Plot the current network """
        labels = {node : f"{attrs['type']}:{attrs['id']}" for node,attrs in self.network.nodes.items()} # short id for nodes
        colors = ['blue' if attrs['type'] == 'schema' else 'red' for attrs in self.network.nodes.values()]
        flayout = getattr(networkx,layout)
        networkx.draw_networkx(self.network,pos=flayout(self.network),with_labels=True,labels=labels,node_color=colors)
        plt.show()

    def dapps_from(self,from_dapp : dapp_proxy.DAppProxy, principal : principals.Principal) -> typing.Iterable[dapp_proxy.DAppProxy]: 
        return [n for n in networkx.descendants(self.network,from_dapp) if isinstance(n,dapp_proxy.DAppProxy)]

    def dapps_required(self,for_dapp : dapp_proxy.DAppProxy, principal : principals.Principal) -> tuple[set[dapp_proxy.DAppProxy],set[dapp_proxy.DAppProxy]]: 
        """ return two sets of DApps required by this DApp
            -   all required 
            -   required for cost calculation, considering despite schemas.Requires annotations, for which
                we take only the longest line of requires DApps as a conservative estimate.

        """
        g = self.network
        sp = networkx.shortest_path(g, target = for_dapp)
        
        lines = [{x for x in l if isinstance(x,dapp_proxy.DAppProxy)} for l in sp.values()]
        suggested = set.union(*lines)

        # node: req if node has any requirement:
        nodes_with_reqs = {k:req for k in sp.keys() if (nk:=g.nodes[k]) ['type'] == 'schema' and (req:=nk.get('requires'))}
        if nodes_with_reqs:
            discard = set() # lines to be discarded, cumulative for all nodes
            for node in nodes_with_reqs:
                req = nodes_with_reqs[node]
                if req in (schemas.Requires.one,schemas.Requires.specific,schemas.Requires.few): # use longest line unless .all
                    # len, line if attributed schema is in the path:
                    ll = [(len(v),i) for i,(k,v) in enumerate(sp.items())  if node in v]
                    retain = max(ll) # longest line (higher index for ties)
                    discard.update({d[1] for d in ll if retain != d}) # others are to be discarded
            lines = [l for i,l in enumerate(lines) if i not in discard] # all lines except those discarded
            calculated = set.union(*lines)
        else: # no attributes, we use all nodes
            calculated = suggested
        return suggested,calculated

    def complete_graph(self):
        """ Finish up the graph after all nodes have been added:
            
            Keys are provided if key above it is provided.
            For all keys that are requires, 
            check if a parent key is provided, and add a provision from this key to that parent
            
        """
        # schema nodes that have an out_edge of type 'requires':
        required_nodes = {n for n,n_out,t in self.network.out_edges((node for node,typ in self.network.nodes(data='type',default=None) if typ =='schema'),data='type') if t=='requires'} # type:ignore

        # schema nodes that have in in_edge of type requires
        provided_nodes = {n for n_in,n,t in self.network.in_edges((node for node,typ in self.network.nodes(data='type',default=None) if typ =='schema'),data='type') if t=='provides'}  # type:ignore

        for node in required_nodes: 
            up = node 
            while up:= up.up(): # go path up until exhausted
                if up in provided_nodes: # provided at this level?
                    self.network.add_edge(up,node,type='provides') # extend provision from up level to node

        return
