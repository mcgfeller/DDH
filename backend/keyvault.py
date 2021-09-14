""" DDH abstract keyvault
"""
from __future__ import annotations
from abc import abstractmethod
import typing

from core import keys,permissions,nodes
from utils.pydantic_utils import NoCopyBaseModel

class AccessKey(NoCopyBaseModel):
    ...

class UserKey(NoCopyBaseModel):
    ...

class AccessKeyVault(NoCopyBaseModel):
    access_keys : dict[str,AccessKey]

class UserKeyVault(NoCopyBaseModel):

    key_by_principal : dict[permissions.Principal,str]


