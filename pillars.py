""" Pillars upon which DDH is built """
from __future__ import annotations
import core
import typing
import utils
import logging

logger = logging.getLogger(__name__)

import schema_root
import import_modules
import DApps

class Executor(core.NoCopyBaseModel):
    ...

class ClearingHouse(core.NoCopyBaseModel):
    ...


class _DAppManager(core.NoCopyBaseModel):
    """ Provisional DAppManager, loads modules and instantiates DApps.
        Real Manager would orchestrate DApps in their own container.

    """


    def bootstrap(self) :
        for module in import_modules.importAllSubPackages(DApps):
            classname = module.__name__.split('.')[-1]
            cls = getattr(module,classname,None) # class must have same name as module
            if not cls:
                logger.error(f'DApp module {module.__name__} has no DApp class named {classname}.')
            else:
                dapp = cls.bootstrap()
                dnode = dapp.startup()
                logger.error(f'DApp {dapp!r} initialized at {dnode!s}.')
        return

DAppManager = _DAppManager()
DAppManager.bootstrap()
    