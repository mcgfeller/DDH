""" Pillars upon which DDH is built """
from __future__ import annotations
import typing

import logging

logger = logging.getLogger(__name__)

from utils import utils
from core import schema_root,dapp,principals
from frontend import sessions
from utils import import_modules 
import DApps
from utils.pydantic_utils import NoCopyBaseModel

import networkx


class Executor(NoCopyBaseModel):
    ...


class ClearingHouse(NoCopyBaseModel):
    ...

class SchemaNetworkClass():

    def __init__(self):
        self.network = networkx.DiGraph()

SchemaNetwork = SchemaNetworkClass()

class DAppManagerClass(NoCopyBaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById : dict[principals.DAppId,dapp.DApp] = {} # registry of DApps

    def bootstrap(self, pillars:dict) :
        session = sessions.get_system_session()
        for module in import_modules.importAllSubPackages(DApps):
            classname = module.__name__.split('.')[-1]
            cls = getattr(module,classname,None) # class must have same name as module
            if not cls:
                logger.error(f'DApp module {module.__name__} has no DApp class named {classname}.')
            else:
                try:
                    dapp = cls.bootstrap(session,pillars)
                except Exception as e:
                    logger.error(f'DApp {cls.__name__} bootstrap error: {e}')
                else:
                    self.DAppsById[dapp.id] = dapp
                    try:
                        dnode = dapp.startup(session,pillars)
                        logger.info(f'DApp {dapp!r} initialized at {dnode!s}.')
                    except Exception as e:
                        logger.error(f'DApp {dapp!r} startup error: {e}')
                    
        return





DAppManager = DAppManagerClass()


Pillars = { # collect the singletons so we can pass them to whomever needs them for their initialization
    'DAppManager':DAppManager,
    'SchemaNetwork' : SchemaNetwork,
    }

DAppManager.bootstrap(Pillars)   