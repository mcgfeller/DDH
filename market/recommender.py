""" Recommender System, used in the Market to recommend Data Apps """

from __future__ import annotations
from re import sub
import typing
import logging
import operator


logger = logging.getLogger(__name__)

import pydantic
from utils import utils
from utils import fastapi_utils
from core import dapp_attrs, schemas, principals, keys, pillars, common_ids, schema_root
from utils.pydantic_utils import DDHbaseModel


class SearchResultItem(DDHbaseModel):
    """ a single search result, with some search information """
    da: dapp_attrs.DApp = pydantic.Field(alias='dapp')
    cost: float = 0.0
    ignored_labels: frozenset[str] = frozenset()  # query labels that have been ignored
    merit: int = pydantic.Field(default=0, description="Ranking merit, starts at 0")
    requires: set[principals.DAppId] = set()
    missing: set[principals.DAppId] = set()

    def __init__(self, *a, **kw):
        """ Because .da is declared as Any, it is not converted; so do it here. 
        """
        super().__init__(*a, **kw)
        if isinstance(self.da, dict):
            self.da = dapp_attrs.DApp(**self.da)
        return


def list_subscriptions(user, all_dapps, sub_dapps) -> typing.Sequence[dapp_attrs.DAppOrFamily]:
    subscribed = [dapp for dapp in all_dapps if dapp.id in sub_dapps]
    return subscribed


async def search_dapps(session, all_dapps: typing.Sequence[dapp_attrs.DAppOrFamily], sub_dapp_ids: frozenset[str], query: str | None,
                       categories: typing.Iterable[common_ids.CatalogCategory | None],
                       desired_labels: typing.Iterable[common_ids.Label | None]) -> list[SearchResultItem]:

    subscribed = list_subscriptions(session.user, all_dapps, sub_dapp_ids)
    if query:
        dapps = all_dapps
        if categories:
            dapps = dapps_in_categories(session, dapps, categories)
        dapps = search_text(session, dapps, query)
    elif categories:
        dapps = dapps_in_categories(session, all_dapps, categories)
    elif subscribed:  # no input, propose complementing to subscribed
        dapps = await from_subscribed(session, subscribed)
    else:  # no input at all - currently, list all DApps - but may raise 413 later
        dapps = all_dapps
    if subscribed:
        # eliminate already subscribed:
        dapps = [da for da in dapps if da not in subscribed]

    sris = [SearchResultItem(dapp=da) for da in dapps]

    if desired_labels:
        sris = check_labels(session, sris, frozenset(desired_labels))

    if sris:
        sris = await add_costs(session, sris, subscribed)
        sris = grade_results(session, sris)

    return sris


def dapps_in_categories(session, all_dapps, categories):
    if categories:
        categories = frozenset(categories)
        return (da for da in all_dapps if da.catalog in categories)
    else:
        return all_dapps


def search_text(session, dapps, query):
    dapps = (d for d in dapps if query.lower() in d.searchtext)  # TODO: Real search
    return dapps


async def from_subscribed(session, dapps: typing.Iterable[dapp_attrs.DAppOrFamily]) -> typing.Iterable[dapp_attrs.DAppOrFamily]:
    """ all reachable Data Apps from subscribed Data Apps, with cost of reach """
    dappids = [str(d.id) for d in dapps if isinstance(d, dapp_attrs.DAppOrFamily)]
    if dappids:
        r = await fastapi_utils.submit1_asynch(session, 'http://localhost:8001', '/graph/from/'+'+'.join(dappids)+'?details=True')
        reachable = sum(r, [])
    else:
        reachable = []
    return reachable


def check_labels(session, sris: list[SearchResultItem], desired_labels: frozenset[common_ids.Label]) -> list[SearchResultItem]:
    """ check presence of labels: Mark missing labels and demerit for each missing label  """
    for sri in sris:
        sri.ignored_labels = desired_labels.difference(sri.da.labels)
        sri.merit -= len(sri.ignored_labels)
    return sris


async def add_costs(session, sris: list[SearchResultItem], subscribed: typing.Iterable[dapp_attrs.DAppOrFamily]) -> list[SearchResultItem]:
    """ Calculate cost of dapp in sris, including costs of pre-requisites except for those already 
        subscribed (which get a bonus merit).

        Schemas with schema.Requires attributes except schema.Requires.all get reduced costs. 

    """
    dappids = [sri.da.id for sri in sris]
    to_r = await fastapi_utils.submit1_asynch(session, 'http://localhost:8001', '/graph/to/'+'+'.join(dappids)+'?include_weights=True')

    for sri, (requires, calculated, weights) in zip(sris, to_r):
        # TODO XXX: all required despite schema annotations, require for cost calculation
        # requires, calculated = schemas.SchemaNetwork.dapps_required(sri.da, session.user)
        # print(f'add_costs: {sri=}, {requires=}, {calculated=}, {weights=}')
        sri.requires = set(requires)
        sri.missing = sri.requires - set(subscribed)
        merits = [weights[da] * (-1)**(da in sri.missing)
                  for da in calculated]  # pos merit if subscribed
        sri.merit += sum(merits)  # bonus for those we have
    return sris


def grade_results(session, sris: list[SearchResultItem]) -> list[SearchResultItem]:
    return sorted(sris, key=operator.attrgetter('merit'), reverse=True)  # highest merit first
