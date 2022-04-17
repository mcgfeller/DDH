

import typing

from core import pillars, principals,common_ids,errors


SUBSCRIPTIONS : dict[common_ids.PrincipalId,dict[principals.DAppId,typing.Any]] = {}

def add_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    ):

    da = pillars.DAppManager.DAppsById.get(dappid)
    if not da:
        raise errors.NotFound(f"DApp not found: {dappid}.")
    SUBSCRIPTIONS.setdefault(user,{})[dappid] = {}
    return

def list_subscriptions(
    user: common_ids.PrincipalId,
    ):
    dappids = SUBSCRIPTIONS.setdefault(user,{}).keys()
    das = [pillars.DAppManager.DAppsById.get(dappid) for dappid in dappids]