""" Module with common id types and no additional imports, to avoid circular imports """

import typing
import enum

TrxId = typing.NewType('TrxId', str)
PersistId = typing.NewType('PersistId', str)
PrincipalId = typing.NewType('PrincipalId', str)
SessionId = typing.NewType('SessionId', str)  # identifies the session


@enum.unique
class Label(str, enum.Enum):
    """ labels and designators """
    id = 'id'
    free = 'free'
    anonymous = 'anonymous'
    pseudonymous = 'pseudonymous'
    system = '-system-'

    def __repr__(self): return self.value


@enum.unique
class CatalogCategory(str, enum.Enum):
    """ Top level catalog categories """
    family = 'family'
    employment = 'employment'
    education = 'education'
    living = 'living'
    finance = 'finance'
    health = 'health'
    system = 'system'

    def __repr__(self): return self.value
