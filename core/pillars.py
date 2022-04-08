""" Pillars upon which DDH is built """
from __future__ import annotations
import typing

import logging

logger = logging.getLogger(__name__)

from utils import utils
from core import schema_root,dapp,principals,keys
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

    def plot(self):
        import matplotlib.pyplot as plt
        labels = {node : str(node) if isinstance(node,keys.DDHkey) else node.id for node in self.network.nodes} # short id for nodes
        networkx.draw_networkx(self.network,with_labels=True,labels=labels)
        plt.show()

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
                    dapps = cls.bootstrap(session,pillars) # one class may generate multiple DApps
                except Exception as e:
                    logger.error(f'DApp {cls.__name__} bootstrap error: {e}')
                else:
                    dapps = utils.ensureTuple(dapps)
                    for dapp in dapps:
                        self.DAppsById[dapp.id] = dapp
                        try:
                            dnode = dapp.startup(session,pillars)
                            logger.info(f'DApp {dapp!r} initialized at {dnode!s}.')
                        except Exception as e:
                            logger.error(f'DApp {dapp!r} startup error: {e}',exc_info=True)
                            raise

        # pillars['SchemaNetwork'].plot()
        return





DAppManager = DAppManagerClass()


Pillars = { # collect the singletons so we can pass them to whomever needs them for their initialization
    'DAppManager':DAppManager,
    'SchemaNetwork' : SchemaNetwork,
    }

DAppManager.bootstrap(Pillars)   