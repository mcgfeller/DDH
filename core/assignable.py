""" Qualities that can be assigned to an object """
from __future__ import annotations


import typing


import pydantic
from utils.pydantic_utils import DDHbaseModel
from utils import utils

from . import errors, schemas, permissions, transactions

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


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

            Must overwrite if Assignable carries attributes.
        """
        if self.may_overwrite:
            if other.cancel or self.cancel:
                return None
            else:
                return other
        elif self.cancel and other.may_overwrite:
            return None
        else:  # all other case are equal
            return self


class Applicable(Assignable):
    supports_modes: typing.ClassVar[frozenset[permissions.AccessMode]]   # supports_modes is a mandatory class variable
    only_modes: typing.ClassVar[frozenset[permissions.AccessMode]
                                ] = frozenset()  # This Applicable is restricted to only_modes
    _all_by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    @classmethod
    def __init_subclass__(cls):
        """ register all Capabilities by Mode """
        super().__init_subclass__()
        sm = getattr(cls, 'supports_modes', None)
        assert sm is not None, f'{cls} must have support_modes set'
        [cls._all_by_modes.setdefault(m, set()).add(cls.__name__) for m in sm]
        return

    @classmethod
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode]) -> set[str]:
        """ return the capabilities required for the access modes """
        caps = set.union(set(), *[c for m in modes if (c := cls._all_by_modes.get(m))])
        return caps

    def apply(self,  assignables: Assignables, schema: schemas.AbstractSchema, access: permissions.Access, transaction: transactions.Transaction, subject: Tsubject) -> Tsubject:
        return subject


class Assignables(DDHbaseModel):
    """ A collection of Assignable.
        Assignable is hashable. We merge assignables of same class. 
    """
    assignables: set[Assignable] = set()
    _by_classname: dict[str, Assignable] = {}  # lookup by class name

    def __init__(self, *a, **kw):
        if a:  # shortcut to allow Assignable as args
            kw['assignables'] = set(list(a)+kw.get('assignables', []))
        super().__init__(**kw)
        merged = False
        for assignable in {a._correct_class() for a in self.assignables}:
            name = assignable.__class__.__name__
            if prev := self._by_classname.get(name):  # two with same  name - merge them:
                assignable = prev.merge(assignable)
                merged = True
                if assignable:
                    self._by_classname[name] = assignable
                else:  # merge returned None, remove it
                    self._by_classname.pop(name)
            else:
                self._by_classname[name] = assignable
        if merged:
            # non-unique assignables can be merged or cancelled, so rebuild set:
            self.assignables = set(self._by_classname.values())
        return

    def dict(self, *a, **kw):
        """ Due to https://github.com/pydantic/pydantic/issues/1090, we cannot have a set 
            as a field. We redefine .dict() and take advantage to exclude_defaults in
            the assignables.
        """
        d = dict(self)
        d['assignables'] = [a.dict(exclude_defaults=True) for a in self.assignables]
        assert isinstance(self.assignables, set)
        return d

    def __contains__(self, assignable: Assignable | type[Assignable]) -> bool:
        """ returns whether assignable class or object is in self """
        if isinstance(assignable, Assignable):
            return assignable in self.assignables
        elif issubclass(assignable, Assignable):
            return assignable.__name__ in self._by_classname
        else:
            return False

    def __len__(self) -> int:
        return len(self.assignables)

    def __eq__(self, other) -> bool:
        """ must compare ._by_classname as list order doesn't matter """
        if isinstance(other, Assignables):
            return self.assignables == other.assignables
        else:
            return False

    def merge(self, other: Assignables) -> typing.Self:
        """ return the stronger of self and other assignables, creating a new combined 
            Assignables.
        """
        if self == other:
            return self
        else:  # merge those in common, then add those only in each set:
            s1 = set(self._by_classname)
            s2 = set(other._by_classname)
            # merge, or cancel:
            common = [r for common in s1 & s2 if (
                r := self._by_classname[common].merge(other._by_classname[common])) is not None]
            r1 = [self._by_classname[n] for n in s1 - s2]  # only in self
            r2 = [other._by_classname[n] for n in s2 - s1]  # only in other
            r = self.__class__(assignables=common+r1+r2)
            return r

    def __add__(self, assignable: Assignable | list[Assignable]) -> typing.Self:
        """ add assignable by merging """
        assignables = utils.ensure_tuple(assignable)
        return self.merge(self.__class__(assignables=assignables))

    def effective(self) -> typing.Self:
        """ Eliminate lone cancel directives """
        assignables = {a for a in self.assignables if not a.cancel}
        if len(assignables) < len(self.assignables):
            return self.__class__(*assignables)
        else:
            return self


class Applicables(Assignables):

    def apply(self, subclass: type[Assignable], schema, access, transaction, subject: Tsubject) -> Tsubject:
        """ apply assignables of subclass in turn """
        for assignable in self.select_for_apply(subclass, schema, access, transaction, subject):
            subject = assignable.apply(self, schema, access, transaction, subject)
        return subject

    # def select_for_apply(self, subclass: type[Assignable] | None, schema, access, transaction, data) -> list[Assignable]:
    #     """ select assignable for .apply()
    #         Basisc selection is on subclass membership (if supplied), but may be refined.
    #     """
    #     return [a for a in self.assignables if (not a.cancel) and (subclass is None or isinstance(a, subclass))]

    def select_for_apply(self, subclass: type[Assignable] | None, schema, access, transaction, data) -> list[Assignable]:
        """ select assignable for .apply()
            We select the required capabilities according to access.mode, according
            to the capabilities supplied by this schema. 
        """
        # select name of those in given subclass
        byname = {c for c, v in self._by_classname.items() if
                  (not v.cancel)
                  and (subclass is None or isinstance(v, subclass))
                  and ((not v.only_modes) or access.modes in v.only_modes)
                  }
        # join the capabilities from each mode:
        required_capabilities = Applicable.capabilities_for_modes(access.modes)
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        if byname:
            return [self._by_classname[c] for c in byname.intersection(required_capabilities)] + \
                [v for c in byname if not (v := self._by_classname[c]).supports_modes]
        else:
            return []


NoApplicables = Applicables()
