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
    # TODO: Provides and requires have different DDHKey subclasses (we currently key.cast when adding to the network)
    target: keys.DDHkey
    qualities: set[Quality] = set()

    @classmethod
    def multiple(cls, relation: Relation, *ddhkeys: typing.Iterable[keys.DDHkey], qualities: set[Quality] = set()) -> list['Reference']:
        ddhkeys = utils.ensure_tuple(ddhkeys)
        return [cls(relation=relation, target=k) for k in ddhkeys]

    @classmethod
    def requires(cls, *ddhkeys: typing.Iterable[keys.DDHkeyRange], **kw):
        assert all(isinstance(d, keys.DDHkeyRange) for d in ddhkeys)
        return cls.multiple(Relation.requires, *ddhkeys, **kw)

    @classmethod
    def provides(cls, *ddhkeys: typing.Iterable[keys.DDHkeyVersioned], **kw):
        assert all(isinstance(d, keys.DDHkeyVersioned) for d in ddhkeys)
        return cls.multiple(Relation.provides, *ddhkeys, **kw)

    @classmethod
    def defines(cls, *ddhkeys: typing.Iterable[keys.DDHkeyVersioned], **kw):
        assert all(isinstance(d, keys.DDHkeyVersioned) for d in ddhkeys)
        return cls.multiple(Relation.defines, *ddhkeys, **kw)


class Quality(DDHbaseModel):
    ...


Reference.model_rebuild()
