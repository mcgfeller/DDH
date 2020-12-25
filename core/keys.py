""" DDH Core Key Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel



class _RootType(str):
    """ Singleton root marker """
    def __repr__(self):
        return '<Root>'

    def __str__(self):
        return ''



class DDHkey(NoCopyBaseModel):
    """ A key identifying a DDH ressource. DDHkey is decoupled from any permissions, storage, etc.,
    """
    
    key : tuple
    node: typing.Optional[nodes.Node] = None

    Delimiter : typing.ClassVar[str] = '/'
    Root : typing.ClassVar[_RootType] = _RootType(Delimiter)

    def __init__(self,key : typing.Union[tuple,list,str], node :  typing.Optional[nodes.Node] = None):
        """ Convert key string into tuple, eliminate empty segments, and set root to self.Root """
        if isinstance(key,str):
            key = key.split(self.Delimiter)
        if len(key) == 0:
            key = () # ensure tuple
        elif not key[0]: # replace root delimiter with root object
            key = (self.Root,)+tuple(filter(None,key[1:]))
        else:
            key = tuple(filter(None,key))
        super().__init__(key=key,node=node)
        return 

    def __str__(self) -> str:
        return self.Delimiter.join(map(str,self.key))

    def __repr__(self) -> str:
        return f'DDHkey({self.Delimiter.join(map(str,self.key))})'

    def __iter__(self) -> typing.Iterator:
        """ Iterate over key """
        return iter(self.key)

    def __getitem__(self,ix) -> typing.Union[tuple,str]:
        """ get part of key, str if ix is integer, tuple if slice """
        return self.key.__getitem__(ix)


    def up(self) -> typing.Optional[DDHkey]:
        """ return key up one level, or None if at top """
        upkey = self.key[:-1]
        if upkey:
            return self.__class__(upkey)
        else: 
            return None

    def split_at(self,split : int) -> typing.Tuple[DDHkey,DDHkey]:
        """ split the key into two DDHkeys at split """
        return self.__class__(self.key[:split]),self.__class__(self.key[split:])

    def ensure_rooted(self) -> DDHkey:
        """ return a DHHkey that is rooted """
        if not self.key[0] == self.Root:
            return self.__class__((self.Root,)+self.key)
        else:
            return self


from . import nodes
DDHkey.update_forward_refs()