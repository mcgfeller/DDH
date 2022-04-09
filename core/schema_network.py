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

    def plot(self):
        """ Plot the current network """
        labels = {node : attrs['id'] for node,attrs in self.network.nodes.items()} # short id for nodes
        colors = ['blue' if attrs['type'] == 'schema' else 'red' for attrs in self.network.nodes.values()]
        networkx.draw_networkx(self.network,pos=networkx.circular_layout(self.network),with_labels=True,labels=labels,node_color=colors)
        plt.show()

    def dapps_from(self,from_dapp : dapp.DApp, principal : principals.Principal) -> typing.Iterable[typing.Iterable[dapp.DApp]]: 
        return [[]]

    def dapps_required(self,dapp : dapp.DApp, principal : principals.Principal) -> typing.Iterable[typing.Iterable[dapp.DApp]]: 
        """ return a sequence of interables of all DApps required by this DApp, highest preference first. 
        """
        return [[]]

