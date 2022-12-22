""" Pillars upon which DDH is built """
from __future__ import annotations
import typing

import logging
from standard_schemas import schema_root

logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_proxy, schema_network, principals,keys
from frontend import sessions
from utils import import_modules 
import DApps
from utils.pydantic_utils import NoCopyBaseModel

import networkx


class Executor(NoCopyBaseModel):
    ...


class ClearingHouse(NoCopyBaseModel):
    ...




Pillars = { # collect the singletons so we can pass them to whomever needs them for their initialization
    'DAppManager':dapp_proxy.DAppManager,
    'SchemaNetwork' : schema_network.SchemaNetworkClass(),
    }

dapp_proxy.DAppManager.bootstrap(Pillars)   