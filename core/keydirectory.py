
""" Directory mapping keys to nodes """
from __future__ import annotations
from abc import abstractmethod
import pydantic 
import datetime
import typing


from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import nodes, keys, transactions


class _NodeRegistry:
    """ Preliminary holder of nodes 
        Note that Nodes are held per NodeSupports, duplicating them as required 
        for easy lookup by NodeSupports.

        A proper realization could use a PatriciaTrie.
    """

    nodes_by_key : dict[tuple,dict[nodes.NodeSupports,nodes.PersistableProxy]] # by key, then by NodeTypes

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : keys.DDHkey, node: nodes.NodeOrProxy):
        """ Store the node, with a reference per NodeSupports """
        node.key = key
        proxy = node.get_proxy()
        by_supports = self.nodes_by_key.setdefault(key.key,{}) 
        for s in proxy.supports:
            by_supports[s] = proxy
        return 


    def __getitem__(self,key : keys.DDHkey) -> dict[nodes.NodeSupports,nodes.PersistableProxy]:
        return self.nodes_by_key.get(key.key,{}) 

    def get_next_proxy(self,key : typing.Optional[keys.DDHkey], support: nodes.NodeSupports) -> typing.Iterator[typing.Tuple[nodes.NodeOrProxy,int]]:
        """ Generator getting next node walking up the tree from key.
            Also indicates at which point the keys.DDHkey is to be split so the first part is the
            path leading to the Node, the 2nd the rest. 
            """
        split = len(key.key) # where to split: counting backwards from the end. 
        while key:
            by_supports =  self[key]
            key = key.up() 
            split -= 1
            nop = by_supports.get(support) # required support?
            if nop:
                yield nop,split+1
        else:
            return

    def get_proxy(self,key : keys.DDHkey,support : nodes.NodeSupports) -> typing.Tuple[typing.Optional[nodes.NodeOrProxy],int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        nop,split = next(( (node,split) for node,split in self.get_next_proxy(key, support) ),(None,-1))
        return nop,split

    def get_node(self,key : keys.DDHkey,support : nodes.NodeSupports, transaction: transactions.Transaction) -> typing.Tuple[typing.Optional[nodes.Node],int]:
        nop,split = self.get_proxy(key,support)
        if nop:
            nop = nop.ensure_loaded(transaction)
            assert isinstance(nop,nodes.Node) # searchable Persistable must be Node
        return nop,split

    


    

NodeRegistry = _NodeRegistry()