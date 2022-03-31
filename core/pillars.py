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


class _DAppManager(NoCopyBaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """
    DAppsById : dict[principals.DAppId,dapp.DApp] = {} # registry of DApps

    def bootstrap(self) :
        session = sessions.get_system_session()
        for module in import_modules.importAllSubPackages(DApps):
            classname = module.__name__.split('.')[-1]
            cls = getattr(module,classname,None) # class must have same name as module
            if not cls:
                logger.error(f'DApp module {module.__name__} has no DApp class named {classname}.')
            else:
                try:
                    dapp = cls.bootstrap(session)
                except Exception as e:
                    logger.error(f'DApp {cls.__name__} bootstrap error: {e}')
                else:
                    self.DAppsById[dapp.id] = dapp
                    try:
                        dnode = dapp.startup(session)
                        logger.info(f'DApp {dapp!r} initialized at {dnode!s}.')
                    except Exception as e:
                        logger.error(f'DApp {dapp!r} startup error: {e}')
                    
        return

DAppManager = _DAppManager()
DAppManager.bootstrap()

class _SchemaNetwork():

    def __init__(self):
        self.network = networkx.DiGraph()

    
SchemaNetwork = _SchemaNetwork()