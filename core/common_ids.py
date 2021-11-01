""" Module with common id types and no additional imports, to avoid circular imports """

import typing

TrxId = typing.NewType('TrxId',str)
PersistId = typing.NewType('PersistId', str)
PrincipalId = typing.NewType('PrincipalId', str)
SessionId = typing.NewType('SessionId', str) # identifies the session