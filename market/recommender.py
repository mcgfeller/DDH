""" Recommender System, used in the Market to recommend Data Apps """

from __future__ import annotations
import typing
import logging

logger = logging.getLogger(__name__)

from utils import utils
from core import schema_network, schema_root,dapp,principals,keys,pillars
from utils.pydantic_utils import NoCopyBaseModel



def search_dapps(session,query):
    dapps = pillars.DAppManager.DAppsById.values()
    if query:
        dapps = search_text(session,dapps,query)
    dapps = order_dapps(session,dapps)
    return dapps

def search_text(session,dapps,query):
    dapps = (d for d in dapps if query.lower() in d.description.lower()) # TODO: Real search
    return dapps

def order_dapps(session,dapps):
    schemaNetwork = pillars.Pillars['SchemaNetwork']
    dapps = list(dapps) # XXX
    deps = {d:schemaNetwork.dapps_required(d,session.user) for d in dapps if isinstance(d,dapp.DApp)}
    return  dapps