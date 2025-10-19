""" Traits that can be assigned to an object.

    Hierarchy:

    Trait 
        Transformer that transforms (or create or check) data
            Capability of a schema, required by modes
            Transformer on a schema
                Validation
                Load and Store        
    Privilege that can be checked

    Collections:
        Traits[Trait]
        Transformers[Transformer]


"""


import enum
import graphlib
import typing
import threading


import pydantic
from utils.pydantic_utils import DDHbaseModel, CV
from utils import utils

from . import errors, schemas, permissions, transactions, keys, node_types

SingleThreaded = threading.RLock()


@enum.unique
class Phase(str, enum.Enum):
    """ Transformation phase, for ordering """

    first = 'first'
    load = 'load'
    parse = 'parse'
    post_load = 'post load'
    validation = 'validation'
    pre_store = 'pre store'
    store = 'store'
    last = 'last'
    none_ = 'none_'  # special phase, not in any sequence


""" Ordered sequences of phases, per mode. Note: first,last must always be present """
Sequences: dict[permissions.AccessMode | None, list[Phase]] = {
    permissions.AccessMode.read: [Phase.first, Phase.load, Phase.post_load, Phase.last],
    permissions.AccessMode.write: [Phase.first, Phase.parse, Phase.post_load, Phase.validation, Phase.pre_store, Phase.store, Phase.last],
    None: [Phase.first, Phase.last]
}


Tsubject = typing.TypeVar('Tsubject')  # subject of apply


class Trait(DDHbaseModel, typing.Hashable):
    """ A Trait is an object, optionally with elements, that can be assigned to another object.
        Traits can be policies, privilges, capabilities, validations, etc. 
        If Traits have an .apply() method transforming data, use the Transformer subclass.

        Traits can be cancelled; multiple Trait objects are collected as Traits.
        Traits can be merged, so the is only one Trait class in Traits. 

        Traits are designed to be inhertible (not from superclass, but from schema) by merging them. 
    """
    model_config = pydantic.ConfigDict(frozen=True, validate_default=True)

    # keep a class by classname, so we can recreate JSON'ed object in correct class
    _cls_by_name: CV[dict[str, type]] = {}

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
            return cls(**self.model_dump())
        else:
            return self

    def __init__(self, *a, **kw):
        """ Ensure classname records name of concrete class, so we can JSON object and back. """
        if 'classname' not in kw:
            kw['classname'] = self.__class__.__name__
        super().__init__(*a, **kw)

    def __invert__(self) -> typing.Self:
        """ invert the cancel flag """
        d = self.model_dump()
        d['cancel'] = True
        return self.__class__(**d)

    def merge(self, other: Trait) -> Trait | None:
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

    def compile(self, traits: Traits, trstate: TransformerState):
        """ Compile this trait, possibly modifying the composite traits this is a part of """
        self.expand_traits(traits, trstate)
        return

    def additional_traits(self) -> list[Trait]:
        """ return additional traits required, called during compile phase """
        return []

    def expand_traits(self, traits: Traits, trstate: TransformerState):
        """ Add traits not yet present, but returned by .additional_traits().
            It usually sufficces to overwrite .additional_traits(), not this method. 
        """
        add = [t for t in self.additional_traits() if t not in traits.traits]  # not present
        if add:
            before = traits._compiled  # we don't need recompilation because of this
            traits += add  # in place merging, sets .compiled flag
            traits._compiled = before
        return


class Transformer(Trait):
    supports_modes: CV[frozenset[permissions.AccessMode]]   # supports_modes is a mandatory class variable
    only_modes: frozenset[permissions.AccessMode
                          ] = frozenset()  # This Transformer is restricted to only_modes
    only_forks: CV[frozenset[keys.ForkType]] = frozenset()  # This Transformer is restricted to only_forks
    _all_by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    phase: CV[Phase]  # phase in which transformer executes, for ordering.
    # after Transformer preceedes this one (within the same phase), for ordering.
    after: str | None = None

    @classmethod
    def __init_subclass__(cls):
        """ register all Capabilities by Mode """
        super().__init_subclass__()
        sm = getattr(cls, 'supports_modes', None)
        assert sm is not None, f'{cls} must have support_modes set'
        [cls._all_by_modes.setdefault(m, set()).add(cls.__name__) for m in sm]
        return

    @classmethod
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode], fork: keys.ForkType) -> set[str]:
        """ return the capabilities required for the access modes and fork """
        caps = set.union(set(), *[c for m in modes if (c := cls._all_by_modes.get(m))])
        # eliminate capabilites for other forks:
        caps = {c for c in caps if not (of := typing.cast(Transformer, cls._cls_by_name[c]).only_forks) or fork in of}
        # eliminate capabilites with mode restrictions:
        caps = {c for c in caps if not (om := typing.cast(
            Transformer, cls._cls_by_name[c]).only_modes) or any(mode in om for mode in modes)}
        return caps

    async def apply(self, traits: Traits, trstate: TransformerState, **kw):
        return

    def __repr__(self) -> str:
        """ Abbreviated output for debugging """
        return self.__class__.__name__


class Traits(DDHbaseModel):
    """ A collection of Trait.
        Trait is hashable. We merge traits of same class. 
    """
    traits: frozenset[Trait] = frozenset()
    _by_classname: dict[str, Trait] = {}  # lookup by class name
    _compiled: bool = False

    def __init__(self, *a, **kw):
        if a:  # shortcut to allow Trait as args
            kw['traits'] = frozenset(list(a)+kw.get('traits', []))
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

    def model_dump(self, *a, **kw):
        """ Due to https://github.com/pydantic/pydantic/issues/1090, we cannot have a set 
            as a field. We redefine .model_dump() and take advantage to exclude_defaults in
            the traits.
        """
        d = dict(self)
        d['traits'] = [a.model_dump(exclude_defaults=True) for a in self.traits]
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
        """ return the combintion of self and other traits, creating a new combined 
            Traits. Traits occuring in self and other are merged. Merging may result
            in None, if one of the traits cancels. 
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

    def __add__(self, trait: Trait | list[Trait] | Traits) -> typing.Self:
        """ add trait by merging, return merged Traits """
        if isinstance(trait, Traits):
            add_traits = trait
        else:
            traits = utils.ensure_tuple(trait)
            add_traits = self.__class__(traits=traits)
        return self.merge(add_traits)

    def __iadd__(self, trait: Trait | list[Trait] | Traits) -> typing.Self:
        """ in place add of traits """
        new_traits = self.__add__(trait)
        for k in ('traits', '_by_classname',):  # this is a Pydantic class, private attribute is now shown
            setattr(self, k, getattr(new_traits, k))
        self._compiled = False
        return self

    def not_cancelled(self) -> typing.Self:
        """ Eliminate lone cancel directives """
        traits = {a for a in self.traits if not a.cancel}
        if len(traits) < len(self.traits):
            return self.__class__(*traits)
        else:
            return self

    def ensure_compiled(self, trstate: TransformerState):
        """ Ensure traits are compiled before use; ._compiled controls this. 
            Ensure that this is only done once, currently globally (could be per class).
        """
        if not self._compiled:  # optimistic, without lock
            with SingleThreaded:
                if not self._compiled:  # check again under lock
                    self._compiled = True  # prevent recursions
                    self.compile(trstate)
        return

    def compile(self, trstate: TransformerState):
        """ Call .compile() on each trait, passing self so compilation can add traits. """
        for trait in self.traits:
            trait.compile(self, trstate)
        return


schemas.AbstractSchema = typing.ForwardRef('schemas.AbstractSchema')


class QueryParams(DDHbaseModel):
    """ QueryParams holder and validator:

        QueryParams are parsed from raw_query_params (a mapping provided in FastAPI request.query_params).
        It can be subclassed for specific schemas (use SchemaAttributes.register_query_params()),
        which allows full validation of schema-specific query parameters. 

        As a downside, any explicitly passed parameter, such as modes, includes_owner, must be included here to 
        avoid validation errors (we don't want to set extra='ignore', as this would not catch mispelled and malicious parameters).

        The validation itself happens in traits.begin_end.QueryParamTransformer, and are passed around in trstate.query_params. 
    """

    model_config = pydantic.ConfigDict(extra='forbid', frozen=True)  # make hashable

    _RegisteredClasses: CV[dict[str, type]] = {}

    # default fields:
    includes_owner: bool = pydantic.Field(
        default=False, description="if set, the body must contain an outer enclosure with the owner id.")
    modes: set[str] | str = pydantic.Field(
        default=set(), description="This parameter is passed explicitly and duplicated here to avoid validation errors.")

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        """ Pydantic variante to register """
        cls._RegisteredClasses[cls.registered_name()] = cls
        return super().__pydantic_init_subclass__(**kwargs)

    @classmethod
    def registered_name(cls) -> str:
        """ The name we use """
        return cls.__module__+'.'+cls.__qualname__

    @classmethod
    def get_class(cls, name: str) -> type[QueryParams]:
        cl = cls._RegisteredClasses.get(name)
        if not cl:
            raise KeyError(f'{name} not registered as QueryParams class')
        return cl


# register superclass - init_subclass is not called for superclass:
QueryParams.__pydantic_init_subclass__()


class TransformerState(DDHbaseModel):
    """ keeps the state during the Transformers chain application """
    nschema: schemas.AbstractSchema = pydantic.Field(alias='schema')
    access: permissions.Access
    transaction: transactions.Transaction
    orig_data: typing.Any
    parsed_data: dict | None = None
    data_node: node_types.T_Node | None = None
    schema_node: node_types.T_SchemaNode | None = None
    raw_query_params: dict | None
    query_params: QueryParams = QueryParams()  # overwritten from raw_query_params in begin_end.QueryParamTransformer
    response_headers: dict = pydantic.Field(
        default_factory=dict, description='Contents will be merged into response header')


class Transformers(Traits):

    async def apply(self,  trstate: TransformerState, subclass: type[Transformer] | None = None, **kw):
        """ apply traits of subclass in turn """
        access = trstate.access
        self.ensure_compiled(trstate)
        traits = self.select_for_apply(access.modes, access.ddhkey.fork, subclass)
        traits = self.sorted(traits, access.modes)
        trait = None  # just for error handling
        try:
            for trait in traits:
                subject = await trait.apply(self, trstate, **kw)
        except Exception as e:
            for abort_trait in DefaultTraits._AbortTransformer.traits:
                assert isinstance(abort_trait, Transformer)
                await abort_trait.apply(self, trstate, failing=trait, exception=e)
            raise  # re-raise exception
        return

    def select_for_apply(self, modes: set[permissions.AccessMode], fork: keys.ForkType, subclass: type[Transformer] | None = None) -> list[Transformer]:
        """ select trait for .apply()
            We select the required capabilities according to access.mode, according
            to the capabilities supplied by this schema. 
        """
        # select name of those in given subclass
        byname = {c for c, v in self._by_classname.items() if
                  (not v.cancel)
                  and ((not v.only_forks) or fork in v.only_forks)
                  and ((not v.only_modes) or modes & v.only_modes)
                  and (subclass is None or isinstance(v, subclass))
                  }
        # join the capabilities from each mode:
        required_capabilities = Transformer.capabilities_for_modes(modes, fork)
        missing = required_capabilities - byname
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        if byname:
            # list with required capbilities according to .supports_modes + list of Transformers without .supports_modes
            trans = [typing.cast(Transformer, self._by_classname[c]) for c in byname.intersection(required_capabilities)] + \
                [v for c in byname if not (v := typing.cast(Transformer, self._by_classname[c])).supports_modes]
        else:
            trans = []
        return trans

    def sorted(self, traits: list[Transformer], modes: set[permissions.AccessMode]) -> list[Transformer]:
        """ return traits sorted according to sequence, and .after settings in individual
            Transformers. 

            Uses topological sorting, as there is no complete order. 
        """
        if len(traits) > 1:
            # get sequence corresponding to mode, or default Sequence if none applies:
            seq = next((s for mode in modes if (s := Sequences.get(mode))), Sequences[None])
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


class _DefaultTraits(DDHbaseModel):
    ready: bool = False  # All traits loaded and ready to use
    # Root validations may be overwritten:
    RootTransformers: Traits = NoTransformers
    NoValidation: Traits = NoTransformers
    HighPrivacyTransformers: Traits = NoTransformers
    HighestPrivacyTransformers: Traits = NoTransformers
    _AbortTransformer: Traits = NoTransformers


DefaultTraits = _DefaultTraits()
