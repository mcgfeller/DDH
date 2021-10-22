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

class _AnyType(str):
    """ Singleton any marker """
    def __repr__(self):
        return '<Any>'

    def __str__(self):
        return ''

@enum.unique
class ForkType(str,enum.Enum):
    """ types of forks """

    data = 'data'
    schema = 'schema'
    consents = 'consents'

    def __repr__(self): return self.value


class DDHkey(NoCopyBaseModel):
    """ A key identifying a DDH ressource. DDHkey is decoupled from any permissions, storage, etc.,
    """
    
    key : tuple
    fork : ForkType = ForkType.data

    # node: typing.Optional[nodes.Node] = None # XXX Used?

    Delimiter : typing.ClassVar[str] = '/'
    ForkDelimiter : typing.ClassVar[str] = ':'
    Root : typing.ClassVar[_RootType] = _RootType(Delimiter)
    AnyKey : typing.ClassVar[_AnyType] = _AnyType(Delimiter)

    def __init__(self,key : typing.Union[tuple,list,str], fork :  typing.Optional[ForkType] = None):
        """ Convert key string into tuple, eliminate empty segments, and set root to self.Root """
        if isinstance(key,str):
            key = key.strip().split(self.Delimiter)
        else:
            key = list(key) # ensure list
        
        if len(key)>1 and not key[0]: # replace root delimiter with root object
            key[0] = self.Root
        if len(key)>2 and not key[1]: # replace empty segment in pos 1 with AllKey
            key[1] = self.AnyKey
        # remove empty segments and make tuple:
        key = tuple(filter(None,key))

        if not fork: # set fork, and remove it from .key if present
            if key and self.ForkDelimiter in key[-1]: # forks are only allowed in last segment
                lk,fork = key[-1].split(self.ForkDelimiter,1) # type:ignore
                key = key[:-1] + (lk,) if lk else () 
            fork = ForkType(fork) if fork else ForkType.data
        super().__init__(key=key,fork=fork) # type:ignore
        return 

    def __str__(self) -> str:
        s = self.Delimiter.join(map(str,self.key))
        if self.fork != ForkType.data:
            s += self.ForkDelimiter+self.fork.value
        return s

    def __repr__(self) -> str:
        return f'DDHkey({self.__str__()})'

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
            return self.__class__(upkey,fork=self.fork)
        else: 
            return None

    def split_at(self,split : int) -> typing.Tuple[DDHkey,DDHkey]:
        """ split the key into two DDHkeys at split """
        return self.__class__(self.key[:split]),self.__class__(self.key[split:])

    def ensure_rooted(self) -> DDHkey:
        """ return a DHHkey that is rooted """
        if len(self.key) < 1  or self.key[0] != self.Root:
            return self.__class__((self.Root,)+self.key,fork=self.fork)
        else:
            return self

    def without_owner(self):
        """ return key without owner """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key != self.AnyKey:
            return self.__class__((self.Root,self.AnyKey)+rooted_key.key[2:],fork=rooted_key.fork)
        else:
            return rooted_key

    @property
    def owners(self) -> str:
        """ return owner as string """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key[0] == self.Root:
            return rooted_key.key[1]
        else:
            return str(self.AnyKey)


from . import nodes
DDHkey.update_forward_refs()