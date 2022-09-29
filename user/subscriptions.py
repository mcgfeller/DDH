

import typing

from core import dapp_attrs,principals,common_ids,errors


SUBSCRIPTIONS : dict[common_ids.PrincipalId,dict[principals.DAppId,typing.Any]] = {}

def add_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    valid_dappids : set[ principals.DAppId],
    ) -> list[principals.DAppId]:

    if not dappid in valid_dappids:
        raise errors.NotFound(f"DApp not found: {dappid}.").to_http()
    SUBSCRIPTIONS.setdefault(user,{})[dappid] = {}
    return list(SUBSCRIPTIONS[user].keys())

def delete_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    valid_dappids : set[ principals.DAppId],
    ) -> list[principals.DAppId]:

    if not dappid in valid_dappids:
        raise errors.NotFound(f"DApp not found: {dappid}.").to_http()
    SUBSCRIPTIONS.setdefault(user,{}).pop(dappid,None)
    return list(SUBSCRIPTIONS[user].keys())

def list_subscriptions(
    user: common_ids.PrincipalId,
    valid_dappids : set[ principals.DAppId],
    ) -> list[dapp_attrs.DApp]:
    dappids = SUBSCRIPTIONS.setdefault(user,{}).keys()
    das = list(dappids) #  [dapp_proxy.DAppManager.DAppsById.get(dappid) for dappid in dappids]
    return das

