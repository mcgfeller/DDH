""" DDH Core Versioning """
from __future__ import annotations


import enum
import functools
import typing
import re
import operator

import pydantic

from utils.pydantic_utils import NoCopyBaseModel

@functools.total_ordering
class Version(NoCopyBaseModel):

    Maxparts : typing.ClassVar[int] = 4


    vtup : tuple[int,...] = ()
    alias : typing.Optional[str] = None
    
    def __init__(self,*v,**kw):
        if v and isinstance(v[0],str):
            vt = v[0].removeprefix('v').split('.')
            if not all(x.isdecimal() for x in vt):
                raise ValueError(f'Invalid version: {v[0]}')
            vtup = tuple(map(int,vt))
        else:
            vtup = tuple(v)
        if not vtup:
            raise ValueError('Empty version')
        kw['vtup'] = (vtup + (0,)*self.Maxparts)[:self.Maxparts]
        super().__init__(**kw)
        return

    def __eq__(self,other):
        if isinstance(other,Version):
            return self.vtup == other.vtup
        elif isinstance(other,str): # fishy?
            try:
                return self == Version(other)
            except: return False
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

class VersionConstraint(NoCopyBaseModel):

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
        return

    def __repr__(self):
        return f'{self.op1}{self.v1}' + (f', {self.op2}{self.v2}' if self.op2 else '')

    def __contains__(self,version :  Version):
        """ returns True is version satisfies VersionConstraint """
        ok = self._vops[self.op1](version,self.v1)
        if ok and self.op2:
            ok = self._vops[self.op2](version,self.v2)
        return ok
