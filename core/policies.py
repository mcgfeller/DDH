""" DDH Core Policy Models """

import pydantic
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import DDHbaseModel

from . import permissions, nodes, trait


class Policy(trait.Trait):
    """ WIP: 
        The policy of a service or a DApp.
        Encompasses the required consent, the update policy
    """

    consents: permissions.Consents | None = permissions.DefaultConsents


EmptyPolicy = Policy()
