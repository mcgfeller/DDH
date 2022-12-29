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


Specifier_types = [ForkType.make_with_default,
                   variant_with_default, versions.Version.make_with_default]


class DDHkey(DDHbaseModel):
    """ A key identifying a DDH ressource. DDHkey is decoupled from any permissions, storage, etc.,
    """
    key: tuple
    fork: ForkType = ForkType.data
    variant: VariantType = DefaultVariant
    version: versions.Version = versions.Unspecified

    Delimiter: typing.ClassVar[str] = '/'
    SpecDelimiter: typing.ClassVar[str] = ':'
    Root: typing.ClassVar[_RootType] = _RootType(Delimiter)
    AnyKey: typing.ClassVar[_AnyType] = _AnyType(Delimiter)

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
                kspecs[i] = Specifier_types[i](k)
            else:
                kspecs[i] = Default_specifiers[i]
        fork, variant, version = kspecs

        super().__init__(key=key, fork=fork, variant=variant, version=version)  # type:ignore
        return

    @property
    def specifiers(self) -> tuple[ForkType, VariantType, versions.Version]:
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
        return f'DDHkey({self.__str__()})'

    def __iter__(self) -> typing.Iterator:
        """ Iterate over key """
        return iter(self.key)

    def __getitem__(self, ix) -> tuple | str:
        """ get part of key, str if ix is integer, tuple if slice """
        return self.key.__getitem__(ix)

    def up(self) -> DDHkey | None:
        """ return key up one level, or None if at top """
        upkey = self.key[:-1]
        if upkey:
            return self.__class__(upkey, specifiers=self.specifiers)
        else:
            return None

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
            return DDHkey(key=self.key, fork=fork, variant=self.variant, version=self.version)
        else:
            return self

    def without_owner(self) -> DDHkey:
        """ return key without owner """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key != self.AnyKey:
            return self.__class__((self.Root, self.AnyKey)+rooted_key.key[2:], specifiers=rooted_key.specifiers)
        else:
            return rooted_key

    def raise_if_no_owner(self):
        if self.owners is self.AnyKey:
            raise errors.NotFound('key has no owner')

    def without_variant_version(self) -> DDHkey:
        """ return key with fork, but without schema variant and version, typically used for access control """
        if self.version == versions.Unspecified and self.variant == DefaultVariant:
            k = self
        else:
            k = self.__class__(self.key, fork=self.fork, variant=DefaultVariant, version=versions.Unspecified)
        return k

    @property
    def owners(self) -> common_ids.PrincipalId:
        """ return owner as string """
        rooted_key = self.ensure_rooted()
        if len(rooted_key.key) > 1 and rooted_key.key[0] == self.Root:
            return rooted_key.key[1]
        else:
            return typing.cast(common_ids.PrincipalId, str(self.AnyKey))


from . import nodes
DDHkey.update_forward_refs()
