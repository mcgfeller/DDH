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

    system_dapps: dict[SystemServices, principals.DAppId] = {
        SystemServices.storage: principals.DAppId('InMemStorageDApp'), }

    def get_dapp(self, system_service: SystemServices):
        from core import dapp_proxy
        dappid = self.system_dapps[system_service]
        dapp = dapp_proxy.DAppManager.DAppsById.get(dappid)
        if not dapp:
            raise errors.NotSelectable(
                f'System {system_service} DApp {dappid} is not available')
        return dapp
