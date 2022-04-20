

import typing

from core import pillars, principals,common_ids,errors,dapp


SUBSCRIPTIONS : dict[common_ids.PrincipalId,dict[principals.DAppId,typing.Any]] = {}

def add_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    ) -> list[principals.DAppId]:

    da = pillars.DAppManager.DAppsById.get(dappid)
    if not da:
        raise errors.NotFound(f"DApp not found: {dappid}.")
    SUBSCRIPTIONS.setdefault(user,{})[dappid] = {}
    return list(SUBSCRIPTIONS[user].keys())

def delete_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    ) -> list[principals.DAppId]:

    da = pillars.DAppManager.DAppsById.get(dappid)
    if not da:
        raise errors.NotFound(f"DApp not found: {dappid}.")
    SUBSCRIPTIONS.setdefault(user,{}).pop(dappid,None)
    return list(SUBSCRIPTIONS[user].keys())

def list_subscriptions(
    user: common_ids.PrincipalId,
    ) -> list[dapp.DApp]:
    dappids = SUBSCRIPTIONS.setdefault(user,{}).keys()
    das = [pillars.DAppManager.DAppsById.get(dappid) for dappid in dappids]
    return das