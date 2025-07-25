""" Set up some Test data """
import asyncio
import datetime
import json
import typing

import pytest
from fastapi.encoders import jsonable_encoder

from backend import keyvault
from utils.pydantic_utils import utcnow
from core import (errors, facade, keydirectory, keys, nodes, permissions,
                  pillars, principals, transactions)
from frontend import sessions, user_auth


def clear_data():
    keydirectory.NodeRegistry._clear({nodes.NodeSupports.data})
    keyvault.clear_vaults()  # need to be independent of other tests


clear_data()


@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')


@pytest.fixture(scope="module")
def user2():
    return user_auth.UserInDB.load('another')


@pytest.fixture(scope="module")
def user3():
    return user_auth.UserInDB.load('another3')


def get_session(user):
    return sessions.Session(token_str='test_session_'+user.id, user=user)


async def write_with_consent(ddhkey: keys.DDHkey | str, consented_users: list[principals.Principal | str] = [], consent_modes: set[permissions.AccessMode] = {permissions.AccessMode.read}, consent_days: int | None = None, no_storage_dapp=None):
    """ utility to write a document to ddhkey, with optional consent grant
        session user is derived from key owner.
        keys and consented_users may be passed as str.
    """
    if isinstance(ddhkey, str):
        ddhkey = keys.DDHkey(ddhkey)
    user = user_auth.UserInDB.load(ddhkey.owner)
    session = get_session(user)
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)
    if consented_users:
        consented_users = [user_auth.UserInDB.load(c) if isinstance(c, str) else c for c in consented_users]
        # grant read access to consented_users
        date_restriction = permissions.DateRestriction(days=consent_days) if consent_days else None
        consents = permissions.Consent.single(
            grantedTo=consented_users, withModes=consent_modes, withinDates=date_restriction)
        ddhkey_c = ddhkey.ensure_fork(keys.ForkType.consents)
        access = permissions.Access(ddhkey=ddhkey_c, modes={permissions.AccessMode.write})
        await facade.ddh_put(access, session, consents.model_dump_json())
    return


async def write_consents(ddhkey: keys.DDHkey | str, consents: permissions.Consents, no_storage_dapp=None):
    """ utility to write consents to ddhkey
    """
    if isinstance(ddhkey, str):
        ddhkey = keys.DDHkey(ddhkey)
    user = user_auth.UserInDB.load(ddhkey.owner)
    session = get_session(user)
    ddhkey_c = ddhkey.ensure_fork(keys.ForkType.consents)
    access = permissions.Access(ddhkey=ddhkey_c, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, consents.model_dump_json())
    return


async def read(ddhkey: keys.DDHkey | str, session: sessions.Session, modes: set[permissions.AccessMode] = {permissions.AccessMode.read}, check_empty: bool = True, raw_query_params: dict = {}) -> tuple[typing.Any, dict]:
    if isinstance(ddhkey, str):
        ddhkey = keys.DDHkey(ddhkey)
    access = permissions.Access(ddhkey=ddhkey, modes=modes)
    data, header = await facade.ddh_get(access, session, raw_query_params=raw_query_params)
    j = jsonable_encoder(data)  # result must be jsonable
    if check_empty:
        assert data, 'data must not be empty'
    return data, header


@pytest.mark.asyncio
async def test_write_data(no_storage_dapp):
    """ test write through facade.ddh_put() """
    await write_with_consent("/mgf/org/private/documents/doc1")

    return


@pytest.mark.asyncio
async def test_write_data_other_owner(user3, no_storage_dapp):
    """ test write through facade.ddh_put() using another owner"""
    session = get_session(user3)
    ddhkey = keys.DDHkey(key="/another/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    with pytest.raises(errors.AccessError):
        await facade.ddh_put(access, session, data)
    return


@pytest.mark.asyncio
async def test_set_consent_top(user, user2, no_storage_dapp):
    """ test set consent at top """
    session = get_session(user)
    trx = session.get_or_create_transaction()
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    await facade.ddh_put(access, session, consents.model_dump_json())
    await trx.commit()
    # now read it back
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.read})
    cj, h = await facade.ddh_get(access, session)
    new_consents = permissions.Consents.model_validate(cj)
    assert consents == new_consents
    return


@pytest.mark.asyncio
async def test_set_consent_deep(user, user2, user3, no_storage_dapp):
    """ test set consent deeper in tree """
    session = get_session(user)
    # first set at top:
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    await facade.ddh_put(access, session, consents.model_dump_json())

    # now withdraw access for user2 to a specific document, but give user3 access:
    ddhkey2 = keys.DDHkey(key="/mgf/org/private/documents:consents")
    access2 = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    consents2 = permissions.Consents(consents=[permissions.Consent(grantedTo=[user3])])
    await facade.ddh_put(access2, session, consents2.model_dump_json())


@pytest.mark.asyncio
async def test_write_data_with_consent(no_storage_dapp):
    """ test write through facade.ddh_put() with three objects:
        - mgf/.../doc1
        - another/.../doc2 with read grant to mgf
        - another/.../doc3
    """
    await write_with_consent("/mgf/org/private/documents/doc1")
    await write_with_consent("/another/org/private/documents/doc2", consented_users=['mgf'])
    await write_with_consent("/another/org/private/documents/doc3")
    # consent shared
    await write_with_consent("/another3/org/private/documents/doc4", consented_users=['mgf', 'another'])
    # consent combinable, may be merged with doc from another
    await write_with_consent("/another3/org/private/documents/doc5", consented_users=['mgf'], consent_modes={permissions.AccessMode.read, permissions.AccessMode.combined, })
    await write_with_consent("/another3/org/private/documents/doc6", consented_users=['mgf'])

    return


@pytest.mark.asyncio
async def test_read_timed_consent(user, user3, no_storage_dapp):
    test_key = keys.DDHkeyGeneric("/another3/org/private/documents/doc7")
    await write_with_consent(test_key, consented_users=['mgf'], consent_days=2)
    session = get_session(user)
    d, header = await read(test_key, session)
    assert 'Expires' in header

    # read with patched access.time in the future:
    access = permissions.Access(ddhkey=test_key)
    access.time = utcnow() + datetime.timedelta(days=5)  # in the future
    with pytest.raises(errors.AccessError, match='must be before'):
        data, header = await facade.ddh_get(access, session)
    return


@pytest.mark.asyncio
async def test_withdraw_consent(user, user3, no_storage_dapp):
    test_key = keys.DDHkeyGeneric("/another3/org/private/documents/doc8")
    session = get_session(user)
    d, h = await read("/mgf/org/ddh/consents/received", session, check_empty=False)
    assert test_key not in d, f'user {user.id} must not have any received consents on key {test_key}'

    # grant read and combined consent to mgf, lise, and laura:
    consent_modes = {permissions.AccessMode.read, permissions.AccessMode.combined, }
    await write_with_consent(test_key, consented_users=['mgf', 'lise', 'laura'], consent_modes=consent_modes)

    # set new consents that remove write consent from lise and all consent from laura:
    consents = permissions.Consents(consents=[
        permissions.Consent(grantedTo=[user_auth.UserInDB.load('lise')], withModes={permissions.AccessMode.read, }),
        permissions.Consent(grantedTo=[user_auth.UserInDB.load('mgf')], withModes={
                            permissions.AccessMode.read, permissions.AccessMode.combined, }),
    ])
    await write_consents(test_key, consents)

    # read back consents given:
    session_another3 = get_session(user_auth.UserInDB.load(test_key.owner))
    d, h = await read("/another3/org/ddh/consents/given", session_another3)

    # check mgf still has consent_modes access:
    d, h = await read("/mgf/org/ddh/consents/received", session)
    assert d.grants[str(test_key)].consents[0].withModes == consent_modes

    # read consent node:
    c, h = await read(test_key.ensure_fork(keys.ForkType.consents), session)

    # get consent node for lise:
    session_lise = get_session(user_auth.UserInDB.load('lise'))
    d, h = await read("/lise/org/ddh/consents/received", session_lise)
    assert d.grants[str(test_key)].consents[0].withModes == {permissions.AccessMode.read, }

    # get consent node for laura:
    session_laura = get_session(user_auth.UserInDB.load('laura'))
    d, h = await read("/laura/org/ddh/consents/received", session_laura)
    assert not d.grants.get(str(test_key))  # must be absent, because consent withdrawn
    return


@pytest.mark.asyncio
async def test_read_and_write_data(user, user2, no_storage_dapp):
    session = get_session(user)
    # first, set up some data:
    await test_write_data_with_consent(no_storage_dapp)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()
    assert trx.read_consentees == set()

    await read("/mgf/org/private/documents/doc1", session)

    # we have grant to read:
    await read("/another/org/private/documents/doc2", session)

    # we can write both docs to user
    ddhkeyW1 = keys.DDHkey(key="/mgf/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW1, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'no need to be related'})
    await facade.ddh_put(access, session, data)

    # but not to user2 as user:
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'no need to be related'})
    with pytest.raises(errors.AccessError):
        await facade.ddh_put(access, session, data)
    return


@pytest.mark.asyncio
async def test_read_and_write_data2(user, user2, no_storage_dapp):
    session = get_session(user)
    combined_read = {permissions.AccessMode.read, permissions.AccessMode.combined}

    # first, set up some data:
    await test_write_data_with_consent(no_storage_dapp)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()

    await read("/mgf/org/private/documents/doc1", session)
    assert principals.AllPrincipal.id not in trx.read_consentees, 'we have read object which does not have universal access'

    # we have grant to read:
    await read("/another/org/private/documents/doc2", session)

    with pytest.raises(errors.AccessError):  # combined read is not consented
        await read("/another/org/private/documents/doc2", session, modes=combined_read)

    # we have a grant and it's with an explicit grant to mgf and another, so it can be combined:
    await read("/another3/org/private/documents/doc4", session)

    # we have a grant and it's shared as combined
    await read("/another3/org/private/documents/doc5", session, modes=combined_read)

    # we have a grant and but this cannot be shared with mgf:
    with pytest.raises(transactions.TrxAccessError) as exc_info:
        await read("/another3/org/private/documents/doc6", session)
    # SessionReinitRequired is subclass of TrxAccessError, so have to check specifically:
    assert not isinstance(exc_info.value, transactions.SessionReinitRequired), 'within trx, no reinit required'

    # even with a new transaction
    await session.ensure_new_transaction()
    with pytest.raises(transactions.SessionReinitRequired):
        await read("/another3/org/private/documents/doc6", session)

    # but with a reinit
    await session.reinit()
    # new workspace after re-init - we need to reread the combinable sequence
    await read("/mgf/org/private/documents/doc1", session)
    await read("/another3/org/private/documents/doc4", session)
    await read("/another3/org/private/documents/doc5", session, modes=combined_read)
    await read("/another3/org/private/documents/doc6", session)

    return


@pytest.mark.asyncio
async def test_read_notfound(user, user2, no_storage_dapp):
    session = get_session(user)
    await write_with_consent("/mgf/org/private/documents/doc1")
    with pytest.raises(errors.NotFound):
        await read("/mgf/org/private/documents/doc99", session)
    return


@pytest.mark.asyncio
async def test_consent_api_received(user, user2, no_storage_dapp):
    session = get_session(user)
    d, h = await read("/mgf/org/ddh/consents/received", session)
    return


@pytest.mark.asyncio
async def test_consent_api_given(user, user2, no_storage_dapp):
    session = get_session(user2)
    d, h = await read("/another/org/ddh/consents/given", session)
    return
