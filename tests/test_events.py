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


@pytest.fixture(scope="module")
def user_lise():
    return user_auth.UserInDB.load('lise')


@pytest.fixture(scope="module")
def user_another():
    return user_auth.UserInDB.load('another')


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
                           {'key': '/lise/org/private/documents'},
                           {'key': '/another/org/private/documents'},
                           ]}

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
async def subscribe_consents(user, no_storage_dapp):
    session = test_own_data.get_session(user)
    userid = user.id
    ddhkey = keys.DDHkey(f'/{userid}/org/ddh/events/subscriptions')
    j = {'subscriptions': [{'key': f'/{userid}/org/ddh/consents/received'}, ]}

    data = json.dumps(j)
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, data)

    d, h = await test_own_data.read(ddhkey, session)
    return


@pytest.mark.asyncio
async def test_event_wait_consent_received(user, user_lise, user_another, no_storage_dapp):
    session_lise = test_own_data.get_session(user_lise)
    await subscribe_consents(user_lise, no_storage_dapp)
    # mgf writes consent to create an event:
    await test_own_data.write_with_consent(f"/{user.id}/org/private/documents/doc1", [user_lise])
    await test_own_data.write_with_consent(f"/{user.id}/org/private/documents/doc1a", [user_lise], consent_modes={permissions.AccessMode.read, permissions.AccessMode.anonymous, })
    await test_own_data.write_with_consent(f"/{user.id}/org/private/documents/doc1p", [user_lise], consent_modes={permissions.AccessMode.read, permissions.AccessMode.pseudonym, })
    await test_own_data.write_with_consent(f"/{user.id}/org/private/documents/doc1no", [user_another])
    # now read as lise:
    ddhkey = keys.DDHkey(f'/{user_lise.id}/org/ddh/events/wait/{user_lise.id}/org/ddh/consents/received')
    # wait for events on this key:
    async with asyncio.timeout(5):
        d, h = await test_own_data.read(ddhkey, session_lise, raw_query_params={'nowait': True})
    assert len(d) >= 3
    for ev in d:
        for key in ev.grants_added:
            if key.key[-1] in ('doc1a', 'doc1p'):
                assert key.owner != user.id
            elif key.key[-1] in ('doc1no', ):
                assert False, 'not granted to this user, must not see'
            else:
                assert key.owner == user.id
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
    await test_own_data.write_with_consent("/another/org/private/documents/doc8")
    session = test_own_data.get_session(user)
    ddhkey = keys.DDHkey('/mgf/org/ddh/events/wait/another/org/private/documents')  # no access to this key
    async with asyncio.timeout(5):  # just in case to avoid hangs
        d, h = await test_own_data.read(ddhkey, session, check_empty=False)
    assert not [ev for ev in d if ev.key.owner ==
                'another'], 'returned data for user=another must be empty, because we lack access'

    return
