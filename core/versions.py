""" DDH Core Versioning """
from __future__ import annotations


import enum
import functools
import typing
import re
import operator

import pydantic
import networkx

from utils.pydantic_utils import NoCopyBaseModel

@functools.total_ordering
class Version(NoCopyBaseModel,typing.Hashable):
    """ Versions are tuples of integers, with a max len.
        Short Version like 1.0 are equal to longer version with the same prefix, e.g.
        1.0.1, 1.0.0.1. If you want to be specific, specify the full length, e.g., 1.0.0.0.

        Versions can have an alias, which is ignored, e.g. a fancy name.

        Versions are hashable and totally ordered. 
    """

    Maxparts : typing.ClassVar[int] = 4


    vtup : tuple[pydantic.conint(ge=0,le=100000),...] = ()
    alias : typing.Optional[str] = None
    
    def __init__(self,*v,**kw):
        if v and isinstance(v[0],str):
            vt = v[0].removeprefix('v').split('.')
            if not all(x.isdecimal() for x in vt):
                raise ValueError(f'Invalid version: {v[0]}')
            vtup = tuple(map(int,vt))
        elif v:
            vtup = tuple(v)
        else:
            vtup = tuple(kw.get('vtup',()))
        # if not vtup:
        #     raise ValueError('Empty version')
        kw['vtup'] = vtup[:self.Maxparts]
        super().__init__(**kw)
        return

    def __eq__(self,other):
        """ Versions are considered equal if one is short and the short segments match,
            e.g., 1.0.1 == 1.0
        """
        if isinstance(other,Version):
            l = min(len(self.vtup),len(other.vtup))
            return self.vtup[:l] == other.vtup[:l]
        else:
            return False

    def __gt__(self,other):
        if isinstance(other,Version):
            return self.vtup > other.vtup
        else:
            return False     

    def __hash__(self):
        return hash(self.vtup)   

    def dotted(self):
        return '.'.join(map(str,self.vtup)) + (f' [{self.alias}]' if self.alias else '')


    __repr__ = __str__ = dotted

class _UnspecifiedVersion(Version):
    def __init__(self,*v,**kw):
        kw['vtup'] = ()
        NoCopyBaseModel.__init__(self,**kw)
        return

    def __eq__(self,other):
        """ only equal to unspecified """
        return isinstance(other,_UnspecifiedVersion)


Unspecified = _UnspecifiedVersion(alias='unspecified')

class VersionConstraint(NoCopyBaseModel):
    """ Version constraint specifiy upper and lower bounds of acceptable versions.
        Comparisons can be specified as '==version', '>=version', '>version', and < instead of >.
        Ranges can be specified by two comparisons such as '<version1,>=version2'.
        The main method is version in versionConstraint.
    """

    # Pattern for comparator,  version , [comparator, version]
    _vpat : typing.ClassVar[typing.Any] = re.compile(r'([<=>]+)([0-9.]+),?(?:([<=>]+)([0-9.]+))?\Z') 
    _vops : typing.ClassVar[dict[str,typing.Callable]] = {'<': operator.lt, '<=': operator.le, '==': operator.eq, '>=': operator.ge, '>': operator.gt, }

    v1 : Version
    op1 :  str
    v2 : typing.Optional[Version] = None
    op2 :  typing.Optional[str] = None

    @pydantic.validator('op1','op2')
    def v_op1(cls, v):
        if v and v not in cls._vops:
            raise ValueError(f"Invalid comparison operator: {v}; permitted: {', '.join(cls._vops.keys())}")
        return v

    def __init__(self,*a,**kw):
        if a and isinstance(c:=a[0],str): # version can be supplied as string
            c = c.replace(' ','')
            # parse with regex
            m = self._vpat.match(c)
            if m:
                op1,v1,op2,v2 = m.groups()
                kw.update({'op1':op1, 'v1': Version(v1),  'op2': op2, 'v2': Version(v2) if v2 else None})
            else:
                raise ValueError(f'Invalid VersionConstraint {c}')
    
        super().__init__(**kw)
        self.normalize()
        return

    def normalize(self):
        """ normalize constraint, so > comes before <.
            May raise errors if there is no version between lower is > upper or if duplicate constraints. 
        """
        if self.op1 == '==' and self.op2 or self.op2 == '==' and self.op1:
            raise ValueError('Only one == constraint allowed')
        if '<' in self.op1:
            if self.op2:
                if '>' in self.op2: # swap constraint
                    assert self.v2
                    (self.op1,self.v1,self.op2,self.v2) = (self.op2,self.v2,self.op1,self.v1)
                    if self.v2 < self.v1:
                        raise ValueError(f'No valid version in {self}')  
                elif '<' in self.op2:
                    raise ValueError('Only one < / <= constraint allowed')
        if '>' in self.op1:
            if self.op2:
                if '<' in self.op2: # check versions
                    assert self.v2
                    if self.v2 < self.v1:
                        raise ValueError(f'No valid version in {self}')  
                elif '>' in self.op2:
                    raise ValueError('Only one > / >= constraint allowed')
        return
        
        

    def __repr__(self):
        return f'{self.__class__.__name__}({self!s})'

    def __str__(self):
        return f'{self.op1}{self.v1}' + (f', {self.op2}{self.v2}' if self.op2 else '')

    def __contains__(self,version :  Version):
        """ returns True is version satisfies VersionConstraint """
        if isinstance(version,_UnspecifiedVersion): # unspecified satisfies all constraints
            ok = True
        else:
            ok = self._vops[self.op1](version,self.v1)
            if ok and self.op2:
                ok = self._vops[self.op2](version,self.v2)
        return ok

NoConstraint = VersionConstraint(op1='>=',v1=Version((0))) # everything is bigger than 0

class Upgrader(typing.Protocol):
      def __call__(self, v_from: Version, v_to: Version, *args : list, **kwargs: dict) -> bool: ...


class Upgraders:
    """ Class to hold upgrade functions for a version upgrade for a specific class (neutral to that class) """

    upgraders : dict[tuple[Version,Version],Upgrader]

    def __init__(self):
        self.network = networkx.DiGraph()

    def add_upgrader(self, v_from: Version, v_to: Version, function: typing.Optional[Upgrader]):
        """ Add upgrade function between two versions; None if no upgrade needed """
        if v_from >= v_to:
            raise ValueError(f'source version {v_from} must be < than target version {v_to}; downgrade not allowed.')
        else:
            self.network.add_nodes_from((v_from,v_to))
            self.network.add_edge(v_from,v_to,function=function)

    def upgrade_path(self, v_from: Version, v_to: Version) -> typing.Sequence[Upgrader]:
        if v_from == v_to:
            return [] # no upgrade required
        elif v_from > v_to:
            raise ValueError('downgrade not supported')
        else:
            try:
                nodes = networkx.shortest_path(self.network,source=v_from,target=v_to)
            except networkx.NetworkXException as e:
                raise ValueError(f'Version {v_from} not upgradeable to {v_to}: {e}')
            # we need the edges, and their function attributes
            e = self.network.edges
            # edges are keyed by pair of nodes they connect (ignore where function is None):
            functions = [f for i in range(len(nodes)-1) if (f:= e[(nodes[i],nodes[i+1])].get('function')) is not None]
            return functions



    