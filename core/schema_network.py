""" The Network of Schema Relationsships """
from __future__ import annotations
import typing
import logging

import networkx
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

from utils import utils
from core import schema_root,dapp,principals,keys
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

    def dapps_from(self,from_dapp : dapp.DApp, principal : principals.Principal) -> typing.Iterable[dapp.DApp]: 
        return [n for n in networkx.descendants(self.network,from_dapp) if isinstance(n,dapp.DApp)]

    def dapps_required(self,for_dapp : dapp.DApp, principal : principals.Principal) -> dict[dapp.DApp,int]: 
        """ return an iterable of all DApps required by this DApp, highest preference first. 
        """
        required = {n: distance for n,distance in networkx.shortest_path_length(self.network, target = for_dapp).items() if distance>0 and isinstance(n,dapp.DApp)}
        return required

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

