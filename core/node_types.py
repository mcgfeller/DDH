""" Types only Node, to avoid circular imports """
from __future__ import annotations
import enum
import pydantic


class T_Node(pydantic.BaseModel): ...


class T_SchemaNode(T_Node): ...


class T_ExecutableNode(T_Node): ...


@enum.unique
class NodeSupports(str, enum.Enum):
    """ Node supports protocol """

    schema = 'schema'
    data = 'data'
    execute = 'execute'
    consents = 'consents'

    def __repr__(self): return self.value


@enum.unique
class Ops(str, enum.Enum):
    """ Operations """

    get = 'get'
    post = 'post'
    put = 'put'
    delete = 'delete'

    def __repr__(self): return self.value
