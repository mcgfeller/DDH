""" System Services, invocation of DApps providing a system service """

from __future__ import annotations

import enum

from utils.pydantic_utils import DDHbaseModel
from core import errors, keys, common_ids, principals


@enum.unique
class SystemServices(str, enum.Enum):
    """ Services which can be provided by user configurable DApp """

    storage = 'storage'
    recommender = 'recommender'


class ProfiledServices(DDHbaseModel):

    system_dapps: dict[SystemServices, principals.DAppId] = {}
