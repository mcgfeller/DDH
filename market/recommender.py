""" Recommender System, used in the Market to recommend Data Apps """

from __future__ import annotations
from re import sub
import typing
import logging
import operator

logger = logging.getLogger(__name__)

import pydantic
from utils import utils
from core import schema_network, schema_root,dapp,principals,keys,pillars,common_ids
from user import subscriptions
from utils.pydantic_utils import NoCopyBaseModel


class SearchResultItem(NoCopyBaseModel):
    """ a single search result, with some search information """
    da : dapp.DApp = pydantic.Field(alias='dapp')
    cost : float = 0.0
    ignored_labels : typing.Iterable[str] = [] # query labels that have been ignored
    merit : int = pydantic.Field(0,description="Ranking merit, starts at 0")



def search_dapps(session,query : typing.Optional[str],categories : typing.Optional[typing.Iterable[common_ids.CatalogCategory]],desired_labels : typing.Optional[typing.Iterable[common_ids.Label]]) -> list[SearchResultItem]:
    subscribed = subscriptions.list_subscriptions(session.user)
    if query:
        dapps = dapps_in_categories(session,categories)
        dapps = search_text(session,dapps,query)
    elif categories:
        dapps = dapps_in_categories(session,categories)
    elif subscribed: # no input, propose complementing to subscribed
        dapps = from_subscribed(session,subscribed)
    else: # no input at all - currently, list all DApps - but may raise 413 later 
        dapps = pillars.DAppManager.DAppsById.values()
    if subscribed:
        # eliminate already subscribed:
        dapps = [da for da in dapps if da not in subscribed]

    
    sris = [SearchResultItem(dapp=da) for da in dapps]
    if desired_labels:
        sris = check_labels(session,sris,frozenset(desired_labels))
    sris = add_costs(session,sris,subscribed)
    sris = grade_results(session,sris)

    return sris

def dapps_in_categories(session,categories):
    if categories:
        categories = frozenset(categories)
        return (da for da in pillars.DAppManager.DAppsById.values() if da.catalog in categories)
    else:
        return pillars.DAppManager.DAppsById.values()

def search_text(session,dapps,query):
    dapps = (d for d in dapps if query.lower() in d.searchtext) # TODO: Real search
    return dapps



def from_subscribed(session,dapps : typing.Iterable[dapp.DApp]) -> typing.Iterable[dapp.DApp]:
    """ all reachable Data Apps from subscribed Data Apps, with cost of reach """
    schemaNetwork = pillars.Pillars['SchemaNetwork']
    reachable = sum((schemaNetwork.dapps_from(d,session.user) for d in dapps if isinstance(d,dapp.DApp)),[])
    return reachable


def check_labels(session,sris : list[SearchResultItem],desired_labels : set[common_ids.Label]) -> list[SearchResultItem]:
    """ check presence of labels: Mark missing labels and demerit for each missing label  """
    for sri in sris:
        sri.ignored_labels = desired_labels.difference(sri.da.labels)
        sri.merit -= len(sri.ignored_labels)
    return sris

def add_costs(session,sris : list[SearchResultItem], subscribed  : typing.Iterable[dapp.DApp]) -> list[SearchResultItem]:
    """ Calculate cost of dapp in sris, including costs of pre-requisites except for those already 
        subscribed (which get a bonus merit).
    """
    schemaNetwork = pillars.Pillars['SchemaNetwork']
    for sri in sris:
        requires = schemaNetwork.dapps_required(sri.da,session.user)
        # sri.requires = requires
        sri.merit += len(set(requires).intersection(subscribed)) # bonus for those we have
        # sri.missing = set(requires) - set(subscribed)




    return sris

def grade_results(session,sris : list[SearchResultItem]) -> list[SearchResultItem]:
    return sorted(sris,key=operator.attrgetter('merit'),reverse=True) # highest merit first