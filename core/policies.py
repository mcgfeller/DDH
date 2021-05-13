""" DDH Core Policy Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel,pyright_check

from . import permissions
from . import nodes

@pyright_check
class Policy(NoCopyBaseModel):
    """ WIP: 
        The policy of a service or a DApp.
        Encompasses the required consent, the update policy
    """

    consent : permissions.Consent = None


EmptyPolicy = Policy()