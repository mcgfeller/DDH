""" DDH Core Policy Models """
from __future__ import annotations
import pydantic
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from . import permissions, nodes, assignable


class Policy(assignable.Assignable):
    """ WIP: 
        The policy of a service or a DApp.
        Encompasses the required consent, the update policy
    """

    consents: permissions.Consents | None = permissions.DefaultConsents


EmptyPolicy = Policy()
