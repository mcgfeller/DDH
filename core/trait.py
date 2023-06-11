""" Qualities that can be assigned to an object """
from __future__ import annotations


import typing


import pydantic
from utils.pydantic_utils import DDHbaseModel
from utils import utils

from . import errors, schemas, permissions, transactions

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Trait(DDHbaseModel, typing.Hashable):
    class Config:
        frozen = True  # Traits are not mutable, and we need a hash function to build  a set

    # keep a class by classname, so we can recreate JSON'ed object in correct class
    _cls_by_name: typing.ClassVar[dict[str, type]] = {}

    classname: str | None = None
    may_overwrite: bool = pydantic.Field(
        default=False, description="trait may be overwritten explicitly in lower schema")
    cancel: bool = pydantic.Field(
        default=False, description="cancels this trait in merge; set using ~trait")

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

    def merge(self, other: Trait) -> typing.Self | None:
        """ return the stronger of self and other traits if self.may_overwrite,
            or None if self.may_overwrite and cancels. 

            Must overwrite if Trait carries attributes.
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


class Transformer(Trait):
    supports_modes: typing.ClassVar[frozenset[permissions.AccessMode]]   # supports_modes is a mandatory class variable
    only_modes: typing.ClassVar[frozenset[permissions.AccessMode]
                                ] = frozenset()  # This Transformer is restricted to only_modes
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

    def apply(self,  traits: Traits, schema: schemas.AbstractSchema, access: permissions.Access, transaction: transactions.Transaction, subject: Tsubject) -> Tsubject:
        return subject


class Traits(DDHbaseModel):
    """ A collection of Trait.
        Trait is hashable. We merge traits of same class. 
    """
    traits: set[Trait] = set()
    _by_classname: dict[str, Trait] = {}  # lookup by class name

    def __init__(self, *a, **kw):
        if a:  # shortcut to allow Trait as args
            kw['traits'] = set(list(a)+kw.get('traits', []))
        super().__init__(**kw)
        merged = False
        for trait in {a._correct_class() for a in self.traits}:
            name = trait.__class__.__name__
            if prev := self._by_classname.get(name):  # two with same  name - merge them:
                trait = prev.merge(trait)
                merged = True
                if trait:
                    self._by_classname[name] = trait
                else:  # merge returned None, remove it
                    self._by_classname.pop(name)
            else:
                self._by_classname[name] = trait
        if merged:
            # non-unique traits can be merged or cancelled, so rebuild set:
            self.traits = set(self._by_classname.values())
        return

    def dict(self, *a, **kw):
        """ Due to https://github.com/pydantic/pydantic/issues/1090, we cannot have a set 
            as a field. We redefine .dict() and take advantage to exclude_defaults in
            the traits.
        """
        d = dict(self)
        d['traits'] = [a.dict(exclude_defaults=True) for a in self.traits]
        assert isinstance(self.traits, set)
        return d

    def __contains__(self, trait: Trait | type[Trait]) -> bool:
        """ returns whether trait class or object is in self """
        if isinstance(trait, Trait):
            return trait in self.traits
        elif issubclass(trait, Trait):
            return trait.__name__ in self._by_classname
        else:
            return False

    def __len__(self) -> int:
        return len(self.traits)

    def __eq__(self, other) -> bool:
        """ must compare ._by_classname as list order doesn't matter """
        if isinstance(other, Traits):
            return self.traits == other.traits
        else:
            return False

    def merge(self, other: Traits) -> typing.Self:
        """ return the stronger of self and other traits, creating a new combined 
            Traits.
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
            r = self.__class__(traits=common+r1+r2)
            return r

    def __add__(self, trait: Trait | list[Trait]) -> typing.Self:
        """ add trait by merging """
        traits = utils.ensure_tuple(trait)
        return self.merge(self.__class__(traits=traits))

    def effective(self) -> typing.Self:
        """ Eliminate lone cancel directives """
        traits = {a for a in self.traits if not a.cancel}
        if len(traits) < len(self.traits):
            return self.__class__(*traits)
        else:
            return self


class Transformers(Traits):

    def apply(self, subclass: type[Trait], schema, access, transaction, subject: Tsubject) -> Tsubject:
        """ apply traits of subclass in turn """
        for trait in self.select_for_apply(subclass, schema, access, transaction, subject):
            subject = trait.apply(self, schema, access, transaction, subject)
        return subject

    # def select_for_apply(self, subclass: type[Trait] | None, schema, access, transaction, data) -> list[Trait]:
    #     """ select trait for .apply()
    #         Basisc selection is on subclass membership (if supplied), but may be refined.
    #     """
    #     return [a for a in self.traits if (not a.cancel) and (subclass is None or isinstance(a, subclass))]

    def select_for_apply(self, subclass: type[Trait] | None, schema, access, transaction, data) -> list[Trait]:
        """ select trait for .apply()
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
        required_capabilities = Transformer.capabilities_for_modes(access.modes)
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        if byname:
            return [self._by_classname[c] for c in byname.intersection(required_capabilities)] + \
                [v for c in byname if not (v := self._by_classname[c]).supports_modes]
        else:
            return []


NoTransformers = Transformers()
