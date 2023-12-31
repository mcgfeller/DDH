""" Pillars upon which DDH is built """
from __future__ import annotations
import typing

import logging


logger = logging.getLogger(__name__)

from utils import utils
from core import dapp_proxy, schemas, schema_network, principals, keys, trait, transactions
from frontend import sessions
from utils import import_modules
import DApps
import schema_formats
import standard_schemas
import traits

from utils.pydantic_utils import DDHbaseModel

import networkx


class Executor(DDHbaseModel):
    ...


class ClearingHouse(DDHbaseModel):
    ...


def load_schema_formats():
    import_modules.importAllSubPackages(schema_formats)


def load_standard_schemas():
    import_modules.importAllSubPackages(standard_schemas)
    schemas.SchemaNetwork.valid.invalidate()  # finished


def load_traits():
    import_modules.importAllSubPackages(traits)
    trait.DefaultTraits.ready = True  # all traits registered themselves


def load_schema_root():
    """ Importing builds root """
    from core import schema_root


Pillars = {  # collect the singletons so we can pass them to whomever needs them for their initialization
    'DAppManager': dapp_proxy.DAppManager,
}

dapp_proxy.DAppManager.bootstrap(Pillars)
load_traits()
load_schema_root()
load_schema_formats()
load_standard_schemas()
