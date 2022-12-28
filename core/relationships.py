""" Relationship to AbstractSchema elements """

from __future__ import annotations
import typing
import pydantic
import enum
from utils.pydantic_utils import DDHbaseModel
from core import keys
from utils import utils


@enum.unique
class Relation(str, enum.Enum):
    """ allowed relations """
    defines = 'defines'
    provides = 'provides'
    requires = 'requires'

    def __repr__(self): return self.value


class Reference(DDHbaseModel):
    relation: Relation
    target: keys.DDHkey
    qualities: set[Quality] = set()

    @classmethod
    def multiple(cls, relation: Relation, *ddhkeys: typing.Iterable[keys.DDHkey], qualities: set[Quality] = set()) -> list['Reference']:
        ddhkeys = utils.ensureTuple(ddhkeys)
        return [cls(relation=relation, target=k) for k in ddhkeys]

    @classmethod
    def requires(cls, *a, **kw):
        return cls.multiple(Relation.requires, *a, **kw)

    @classmethod
    def provides(cls, *a, **kw):
        return cls.multiple(Relation.provides, *a, **kw)

    @classmethod
    def defines(cls, *a, **kw):
        return cls.multiple(Relation.defines, *a, **kw)


class Quality(DDHbaseModel):
    ...


Reference.update_forward_refs()
