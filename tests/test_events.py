""" Set up some Test data """
import asyncio
import json

import pytest
from fastapi.encoders import jsonable_encoder

from backend import keyvault
from core import (errors, facade, keydirectory, keys, nodes, permissions,
                  pillars, principals, transactions)
from frontend import sessions, user_auth
from tests import test_own_data


test_own_data.clear_data()


@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')


async def give_lise_consents(no_storage_dapp):
    """ Give consent on top key /mgf to lise """
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=['lise'])])
    r = await test_own_data.write_consents("/mgf", consents, no_storage_dapp)
    return r


@pytest.mark.asyncio
async def test_event_subscribe(user, no_storage_dapp):
    session = test_own_data.get_session(user)
    ddhkey = keys.DDHkey('/mgf/org/ddh/events/subscriptions')
    j = {'subscriptions': [{'key': '/mgf/org/ddh/consents/received'}, {'key': '/mgf/org/private/documents'},
                           {'key': '/mgf/p/living/shopping/receipts'},
                           {'key': '/lise/org/private/documents'}]}

    data = json.dumps(j)
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, data)

    d, h = await test_own_data.read(ddhkey, session)
    return


@pytest.mark.asyncio
async def test_event_wait(user, no_storage_dapp):
    await test_event_subscribe(user, no_storage_dapp)
    session = test_own_data.get_session(user)
    # write something to create an event:
    await test_own_data.write_with_consent("/mgf/org/private/documents/doc1")
    ddhkey = keys.DDHkey('/mgf/org/ddh/events/wait/mgf/org/private/documents')
    # wait for events on this key:
    async with asyncio.timeout(5):
        d, h = await test_own_data.read(ddhkey, session, raw_query_params={'nowait': True})
    return


@pytest.mark.asyncio
async def test_event_wait_nosubscribed(user, no_storage_dapp):
    await test_event_subscribe(user, no_storage_dapp)
    session = test_own_data.get_session(user)
    ddhkey = keys.DDHkey('/mgf/org/ddh/events/wait/mgf/p/living')  # not subscribed to this key
    with pytest.raises(errors.NotFound):
        async with asyncio.timeout(5):  # just in case to avoid hangs
            d, h = await test_own_data.read(ddhkey, session)
    return


@pytest.mark.asyncio
async def test_event_wait_noaccess(user, no_storage_dapp):
    await test_event_subscribe(user, no_storage_dapp)
    # write something to create an event:
    await test_own_data.write_with_consent("/lise/org/private/documents/doc1")
    session = test_own_data.get_session(user)
    ddhkey = keys.DDHkey('/mgf/org/ddh/events/wait/lise/org/private/documents')  # no access to this key
    async with asyncio.timeout(5):  # just in case to avoid hangs
        d, h = await test_own_data.read(ddhkey, session, check_empty=False)
    assert not d, 'returned data must be empty, because we lack access'

    return
