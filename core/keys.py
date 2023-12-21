""" DDH Core Key Models """
from __future__ import annotations
import pydantic
import datetime
import typing
import enum
import abc
import pydantic.json

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel
from core import common_ids, versions, errors


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
class ForkType(str, enum.Enum):
    """ types of forks """

    data = 'data'
    schema = 'schema'
    consents = 'consents'

    def __repr__(self): return self.value

    __str__ = __repr__

    @classmethod
    def make_with_default(cls, v: str | None) -> ForkType:
        return cls(v) if v else cls.data


VariantType = typing.NewType('VariantType', str)
DefaultVariant = VariantType('')
Default_specifiers = [ForkType.data, DefaultVariant, versions.Unspecified]


def variant_with_default(v: str | None) -> VariantType:
    return VariantType(v) if v else DefaultVariant


class DDHkey(DDHbaseModel):
    """ A key identifying a DDH resource. DDHkey is decoupled from any permissions, storage, etc.,
    """
    key: tuple
    fork: ForkType = ForkType.data
    variant: VariantType = DefaultVariant
    version: versions.Version | versions.VersionConstraint = versions.Unspecified

    Delimiter: typing.ClassVar[str] = '/'
    SpecDelimiter: typing.ClassVar[str] = ':'
    OwnerDelimiter: typing.ClassVar[str] = ','
    Root: typing.ClassVar[_RootType] = _RootType(Delimiter)
    AnyKey: typing.ClassVar[_AnyType] = _AnyType(Delimiter)

    Specifier_types: typing.ClassVar[list] = [ForkType.make_with_default,
                                              variant_with_default, versions.make_version_or_constraint]

    def dict(self, **kw):
        """ We want a short representation """
        return {'key': str(self)}

    def __init__(self, key: tuple | list | str, specifiers: typing.Sequence = (), fork:  ForkType | None = None, variant: str | None = None, version:  versions.Version | None = None):
        """ Convert key string into tuple, eliminate empty segments, set root to self.Root, and extract specifiers """
        if isinstance(key, str):
            key = key.strip().split(self.Delimiter)
        else:
            key = list(key)  # ensure list

        if len(key) > 1 and not key[0]:  # replace root delimiter with root object
            key[0] = self.Root

        if len(key) > 2 and not key[1]:  # replace empty segment in pos 1 with AllKey
            key[1] = self.AnyKey

        # remove empty segments and make tuple:
        key = tuple(filter(None, key))

        # supplied + defaults
        # extend to cover all specifiers
        specifiers = list(specifiers) + [None]*(len(Default_specifiers)-len(specifiers))
        for i, v in enumerate((fork, variant, version)):  # add extra arguments
            if v:
                specifiers[i] = v

        if key and self.SpecDelimiter in key[-1]:  # specifiers are only allowed in last segment
            lk, *kspecs = key[-1].split(self.SpecDelimiter, len(Default_specifiers))  # type:ignore
            kspecs = kspecs + [None]*(len(specifiers)-len(kspecs))
            key = key[:-1] + (lk,) if lk else ()  # reassemble key
        else:
            kspecs = [None]*len(specifiers)

        for i, (s, k) in enumerate(zip(specifiers, kspecs)):
            if s:
                kspecs[i] = s
            elif k:
                kspecs[i] = self.Specifier_types[i](k)
            else:
                kspecs[i] = Default_specifiers[i]
        fork, variant, version = kspecs

        super().__init__(key=key, fork=fork, variant=variant, version=version)  # type:ignore
        return

    @property
    def specifiers(self) -> tuple[ForkType, VariantType, versions.Version | versions.VersionConstraint]:
        return (self.fork, self.variant, self.version)

    def __hash__(self):
        return hash((self.key)+self.specifiers)

    def __eq__(self, other):
        if isinstance(other, DDHkey):
            return (self.key == other.key) and (self.specifiers == other.specifiers)
        else:
            return False

    def __str__(self) -> str:
        """ str representation, omitting defaults and truncating trailing ':' """
        s = self.Delimiter.join(map(str, self.key))
        specs = ['' if s == d else str(s) for s, d in zip(self.specifiers, Default_specifiers)]
        p = self.SpecDelimiter+self.SpecDelimiter.join(specs)
        return s+p.rstrip(self.SpecDelimiter)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.__str__()})'

    def __iter__(self) -> typing.Iterator:
        """ Iterate over key """
        return iter(self.key)

    def __getitem__(self, ix) -> tuple | str:
        """ get part of key, str if ix is integer, tuple if slice """
        return self.key.__getitem__(ix)

    def __bool__(self) -> bool:
        return bool(self.key)

    @classmethod
    def cast(cls, key: DDHkey) -> typing.Self:
        """ Cast a key to a specific subclass """
        if not isinstance(key, DDHkey):
            raise ValueError(f'Cannot cast type {key.__class__.__name__} to {cls.__name__}')
        elif cls == key.__class__:
            return key
        else:
            return cls(str(key))

    def up(self, retain_specifiers=False) -> DDHkey:
        """ return key up one level; if a top, bool(key) is False
            If retain_specifiers is True, specifiers and __class__ are retained.
        """
        upkey = self.key[:-1]
        if retain_specifiers:
            cls = self.__class__
            specifiers = self.specifiers
        else:
            cls = DDHkey
            specifiers = ()

        return cls(upkey, fork=self.fork, specifiers=specifiers)

    def split_at(self, split: int) -> typing.Tuple[DDHkey, DDHkey]:
        """ split the key into two DDHkeys at split
            The specifiers go onto the leading segment.
        """
        return self.__class__(self.key[:split], specifiers=self.specifiers), self.__class__(self.key[split:])

    def remainder(self, split: int) -> DDHkey:
        """ return remainder after split, no specifiers """
        return self.__class__(self.key[split:])

    def ensure_rooted(self) -> DDHkey:
        """ return a DDHkey that is rooted """
        if len(self.key) < 1 or self.key[0] != self.Root:
            return self.__class__((self.Root,)+self.key, specifiers=self.specifiers)
        else:
            return self

    def ensure_fork(self, fork:  ForkType) -> typing.Self:
        """ ensure that key has the given fork """
        if self.fork != fork:  # create new key with correct fork
            return self.__class__(key=self.key, fork=fork, variant=self.variant, version=self.version)
        else:
            return self

    def ens(self) -> typing.Self:
        """ enusre key is a valid schema key - shortcut for without_owner().ensure_fork(schema) """
        return self.without_owner().ensure_fork(ForkType.schema)

    def without_owner(self) -> DDHkey:
        """ return key without owner """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key != self.AnyKey:
            return self.__class__((self.Root, self.AnyKey)+rooted_key.key[2:], specifiers=rooted_key.specifiers)
        else:
            return rooted_key

    def raise_if_no_owner(self):
        if self.owner is self.AnyKey:
            raise errors.NotFound('key has no owner')

    def with_new_owner(self, owner: common_ids.PrincipalId) -> typing.Self:
        """ return key with owner set to owner """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key[1] == owner:
            return self
        else:
            return self.__class__((self.Root, owner)+rooted_key.key[2:], specifiers=rooted_key.specifiers)

    def without_variant_version(self) -> DDHkeyGeneric:
        """ return key with fork, but without schema variant and version, typically used for access control """
        if isinstance(self, DDHkeyGeneric):
            k = self
        elif self.version == versions.Unspecified and self.variant == DefaultVariant:
            # Key is actually generic, but we cannot change class for Pydantic objects, so must recreate:
            k = DDHkeyGeneric(self.key, fork=self.fork, variant=DefaultVariant, version=versions.Unspecified)
        else:
            k = DDHkeyGeneric(self.key, fork=self.fork, variant=DefaultVariant, version=versions.Unspecified)
        return k

    @property
    def owner(self) -> common_ids.PrincipalId:
        """ return owner as string """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key[0] == self.Root:
            return rooted_key.key[1]
        else:
            return typing.cast(common_ids.PrincipalId, str(self.AnyKey))

    @property
    def owners(self) -> tuple[common_ids.PrincipalId, ...]:
        """ return tuple of owners, empty tuple if no owner - not (AnyKey,)!  """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key[0] == self.Root:
            if rooted_key.key[1] == self.AnyKey:
                return ()
            else:
                return tuple(rooted_key.key[1].split(self.OwnerDelimiter))
        else:
            return ()

    def longest_segments(self) -> typing.Generator[DDHkey, None, None]:
        """ Generator yielding sucessively shorter subkeys
            specifiers are not copied. Keys are  only built as needed.
        """
        return (DDHkey(self.key[:i]) for i in range(len(self.key), -1, -1))  # count downward from end to 0

    def __add__(self, a: DDHkey | tuple | str) -> typing.Self:
        """ Add a further segment, creating a new key """
        if not a:
            return self
        else:
            if isinstance(a, DDHkey):
                k = a.key
            elif isinstance(a, str):
                k = (a,)
            else:
                k = tuple(a)
            return self.__class__(self.key+k, specifiers=self.specifiers)


class DDHkeyGeneric(DDHkey):
    """ DDHKey which must not contain Variant nor Version """

    # variant: typing.Final[VariantType] = DefaultVariant # TODO: https://github.com/pydantic/pydantic/issues/6253
    # version: typing.Final[versions.Version] = versions.Unspecified
    variant: VariantType = DefaultVariant
    version: versions.Version = versions.Unspecified

    Specifier_types: typing.ClassVar[list] = [ForkType.make_with_default,
                                              variant_with_default, versions.Version.make_with_default]

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        if self.variant != DefaultVariant:
            raise ValueError('DDHkeyGeneric must not have non-default variant')
        if self.version != versions.Unspecified:
            raise ValueError('DDHkeyGeneric must not have non-default version')
        return


class DDHkeyRange(DDHkey):
    """ DDHKey which must Variant and VersionConstraint """

    variant: VariantType = DefaultVariant
    version: versions.VersionConstraint = versions.NoConstraint

    Specifier_types: typing.ClassVar[list] = [ForkType.make_with_default,
                                              variant_with_default, versions.VersionConstraint.make_with_default]

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        # if self.variant == DefaultVariant:
        #     raise ValueError('DDHkeyRange must have non-default variant')
        if (not isinstance(self.version, versions.VersionConstraint)) or self.version == versions.NoConstraint:
            raise ValueError('DDHkeyRange must not have unconstrained version')
        return

    def __contains__(self, key: DDHkeyVersioned):
        """ return True if key fulfills this range """
        ok = key.variant == self.variant and key.version in self.version
        # print(
        #     f'DDHkeyRange.__contains__({self=},{key=}) -> {ok} {key.variant == self.variant=} {key.version in self.version=}')
        return ok


class DDHkeyVersioned(DDHkey):
    """ DDHKey which must contain Variant and Version """

    variant: VariantType = DefaultVariant
    version: versions.Version = versions.Unspecified

    Specifier_types: typing.ClassVar[list] = [ForkType.make_with_default,
                                              variant_with_default, versions.Version.make_with_default]

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        if (not isinstance(self.version, versions.Version)) or self.version == versions.Unspecified:
            raise ValueError('DDHkeyVersioned must not have unspecified version')
        return

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, o):
        return super().__eq__(o)

    def to_range(self) -> DDHkeyRange:
        """ return a range key constraining to this version """
        if self.version == versions.Unspecified:
            return DDHkeyRange(self.key, variant=self.variant, version=versions.NoConstraint)
        else:
            return DDHkeyRange(self.key, variant=self.variant, version=versions.VersionConstraint(op1='==', v1=self.version))


class DDHkeyVersioned0(DDHkeyVersioned):
    """ DDHKey Versioned with default 0 """

    version: versions.Version = versions.Version(0)

    def __init__(self, *a, **kw):
        DDHkey.__init__(self, *a, **kw)
        if (not isinstance(self.version, versions.Version)) or self.version == versions.Unspecified:
            self.version = versions.Version(0)
        return

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, o):
        return super().__eq__(o)


from . import nodes
DDHkey.model_rebuild()
