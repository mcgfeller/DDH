""" Qualities that can be assigned to an object """
from __future__ import annotations


import typing


import pydantic
from utils.pydantic_utils import DDHbaseModel
from utils import utils

from . import errors


class Assignable(DDHbaseModel, typing.Hashable):
    class Config:
        frozen = True  # Assignables are not mutable, and we need a hash function to build  a set

    # keep a class by classname, so we can recreate JSON'ed object in correct class
    _cls_by_name: typing.ClassVar[dict[str, type]] = {}

    classname: str | None = None
    may_overwrite: bool = pydantic.Field(
        default=False, description="assignable may be overwritten explicitly in lower schema")
    cancel: bool = pydantic.Field(
        default=False, description="cancels this assignable in merge; set using ~assignable")

    @classmethod
    def __init_subclass__(cls):
        """ register all potential class names """
        cls._cls_by_name[cls.__name__] = cls

    def _correct_class(self) -> typing.Self:
        """ Recreate JSON'ed object in correct class, based on .classname attribute """
        if self.classname and self.classname != self.__class__.__name__:
            cls = self._cls_by_name[self.classname]
            return cls(**self.dict())
        else:
            return self

    def __init__(self, *a, **kw):
        """ Ensure classname records name of concrete class """
        if 'classname' not in kw:
            kw['classname'] = self.__class__.__name__
        super().__init__(*a, **kw)

    def __invert__(self) -> typing.Self:
        """ invert the cancel flag """
        d = self.dict()
        d['cancel'] = True
        return self.__class__(**d)

    def merge(self, other: Assignable) -> typing.Self | None:
        """ return the stronger of self and other assignables if self.may_overwrite,
            or None if self.may_overwrite and cancels. 
        """
        if self.may_overwrite:
            if other.cancel:
                return None
            else:
                return other
        else:  # all other case are equal
            return self


class Assignables(DDHbaseModel):
    """ A collection of Assignable.
        Assignable is not hashable, so we keep a list and build a dict with the class names. 
    """
    assignables: list[Assignable] = []
    _by_name: dict[str, Assignable] = {}

    def __init__(self, *a, **kw):
        if a:  # shortcut to allow Assignable as args
            kw['assignables'] = list(a)+kw.get('assignables', [])
        super().__init__(**kw)
        self.assignables = [a._correct_class() for a in self.assignables]
        self._by_name = {r.__class__.__name__: r for r in self.assignables}

    def __contains__(self, assignable: type[Assignable]) -> bool:
        """ returns whether assignable class is in self """
        return assignable.__name__ in self._by_name

    def __len__(self) -> int:
        return len(self._by_name)

    def __eq__(self, other) -> bool:
        """ must compare ._by_name as list order doesn't matter """
        if isinstance(other, Assignables):
            return self._by_name == other._by_name
        else:
            return False

    def merge(self, other: Assignables) -> typing.Self:
        """ return the stronger of self and other assignables, creating a new combined 
            Assignables.
        """
        if self._by_name == other._by_name:
            return self
        else:  # merge those in common, then add those only in each set:
            s1 = set(self._by_name)
            s2 = set(other._by_name)
            # merge, or cancel:
            common = [r for common in s1 & s2 if (r := self._by_name[common].merge(other._by_name[common])) is not None]
            r1 = [self._by_name[n] for n in s1 - s2]  # only in self
            r2 = [other._by_name[n] for n in s2 - s1]  # only in other
            r = self.__class__(assignables=common+r1+r2)
            return r

    def __add__(self, assignable: Assignable | list[Assignable]) -> typing.Self:
        """ add assignable by merging """
        assignables = [a._correct_class() for a in utils.ensure_tuple(assignable)]
        return self.merge(self.__class__(assignables=assignables))