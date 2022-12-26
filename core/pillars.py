""" Pillars upon which DDH is built """
from __future__ import annotations
import typing

import logging


logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_proxy, schemas, schema_network, principals,keys,schema_root
from frontend import sessions
from utils import import_modules 
import DApps
import schema_formats
import standard_schemas

from utils.pydantic_utils import NoCopyBaseModel

import networkx


class Executor(NoCopyBaseModel):
    ...


class ClearingHouse(NoCopyBaseModel):
    ...


def load_schema_formats():
    import_modules.importAllSubPackages(schema_formats)

def load_standard_schemas():
    import_modules.importAllSubPackages(standard_schemas)


Pillars = { # collect the singletons so we can pass them to whomever needs them for their initialization
    'DAppManager':dapp_proxy.DAppManager,
    'SchemaNetwork' : schema_network.SchemaNetworkClass(),
    }

dapp_proxy.DAppManager.bootstrap(Pillars)   
load_schema_formats()
load_standard_schemas()

