""" Traits that can be assigned to an object.

    Hierarchy:

    Trait 
        Transformer that transforms (or create or check) data
            Capability of a schema, required by modes
            Restriction on a schema
    Privilege that can be checked

    Collections:
        Traits[Trait]
        Transformers[Transformer]


"""
from __future__ import annotations

import enum
import graphlib
import typing


import pydantic
from utils.pydantic_utils import DDHbaseModel
from utils import utils

from . import errors, schemas, permissions, transactions

Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Trait(DDHbaseModel, typing.Hashable):
    """ A Trait is an object, optionally with elements, that can be assigned to another object.
        Traits can be policies, privilges, capabilities, restrictions, etc. 
        If Traits have an .apply() method transforming data, use the Transformer subclass.

        Traits can be cancelled; multiple Trait objects are collected as Traits.
        Traits can be merged, so the is only one Trait class in Traits. 

        Traits are designed to be inhertible (not from superclass, but from schema) by merging them. 
    """
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
        """ Ensure classname records name of concrete class, so we can JSON object and back. """
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


@enum.unique
class Phase(str, enum.Enum):
    """ Transformation phase, for ordering """

    load = 'load'
    parse = 'parse'
    post_load = 'post load'
    validation = 'validation'
    store = 'store'


""" Ordered sequences of phases, per mode """
Sequences: dict[permissions.AccessMode, list[Phase]] = {
    permissions.AccessMode.read: [Phase.load, Phase.post_load],
    permissions.AccessMode.write: [Phase.parse, Phase.post_load, Phase.validation, Phase.store],
}


class Transformer(Trait):
    supports_modes: typing.ClassVar[frozenset[permissions.AccessMode]]   # supports_modes is a mandatory class variable
    only_modes: typing.ClassVar[frozenset[permissions.AccessMode]
                                ] = frozenset()  # This Transformer is restricted to only_modes
    _all_by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    phase: typing.ClassVar[Phase] = pydantic.Field(
        default=..., description="phase in which transformer executes, for ordering.")
    # after Transformer preceedes this one (within the same phase), for ordering.
    after: typing.ClassVar[str | None] = None

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

    async def apply(self,  traits: Traits, schema: schemas.AbstractSchema, access: permissions.Access, transaction: transactions.Transaction, subject: Tsubject) -> Tsubject:
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

    def not_cancelled(self) -> typing.Self:
        """ Eliminate lone cancel directives """
        traits = {a for a in self.traits if not a.cancel}
        if len(traits) < len(self.traits):
            return self.__class__(*traits)
        else:
            return self


class Transformers(Traits):

    async def apply(self, schema, access: permissions.Access, transaction, subject: Tsubject, subclass: type[Transformer] | None = None) -> Tsubject:
        """ apply traits of subclass in turn """
        traits = self.select_for_apply(access.modes, subclass)
        traits = self.sorted(traits, access.modes)
        for trait in traits:
            subject = await trait.apply(self, schema, access, transaction, subject)
        return subject

    def select_for_apply(self, modes: set[permissions.AccessMode], subclass: type[Transformer] | None = None) -> list[Transformer]:
        """ select trait for .apply()
            We select the required capabilities according to access.mode, according
            to the capabilities supplied by this schema. 
        """
        # select name of those in given subclass
        byname = {c for c, v in self._by_classname.items() if
                  (not v.cancel)
                  and ((not v.only_modes) or modes & v.only_modes)
                  and (subclass is None or isinstance(v, subclass))
                  }
        # join the capabilities from each mode:
        required_capabilities = Transformer.capabilities_for_modes(modes)
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        if byname:
            # list with required capbilities according to .supports_modes + list of Transformers without .supports_modes
            return [typing.cast(Transformer, self._by_classname[c]) for c in byname.intersection(required_capabilities)] + \
                [v for c in byname if not (v := typing.cast(Transformer, self._by_classname[c])).supports_modes]
        else:
            return []

    def sorted(self, traits: list[Transformer], modes: set[permissions.AccessMode]) -> list[Transformer]:
        """ return traits sorted according to sequence, and .after settings in individual
            Transformers. 

            Uses topological sorting, as there is no complete order. 
        """
        if len(traits) > 1:
            seq = next((Sequences.get(mode) for mode in modes), None)  # get sequence corresponding to mode
            if seq:
                # Build sorted sequence of traits and phases. Phases will be eliminated, so prefix them by marker:
                marker = '|'
                # Each trait depends on its phase:
                g = graphlib.TopologicalSorter({trait.classname: {marker+trait.phase}
                                               for trait in traits if trait.phase in seq})
                # Each phase depends on its predecessor, except the first one:
                [g.add(marker+p, marker+seq[i-1]) for i, p in enumerate(seq) if i > 0]
                # Add individual .after dependencies where given:
                [g.add(trait.classname, trait.after)
                 for trait in traits if trait.after and trait.after in self._by_classname]
                # get order, eliminating phases marked by marker
                traits = [typing.cast(Transformer, self._by_classname[c]) for c in g.static_order() if c[0] != marker]

        return traits


NoTransformers = Transformers()
