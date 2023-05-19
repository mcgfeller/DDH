""" Set up some Test data """
from core import keys, permissions, facade, errors, transactions, principals
from core import pillars
from frontend import user_auth, sessions
from backend import keyvault
import pytest
import json

keyvault.clear_vaults()  # need to be independet of other tests


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


@pytest.mark.asyncio
async def test_write_data(user):
    """ test write through facade.ddh_put() """
    session = get_session(user)
    ddhkey = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)

    return


@pytest.mark.asyncio
async def test_write_data_other_owner(user):
    """ test write through facade.ddh_put() using another owner"""
    session = get_session(user)
    ddhkey = keys.DDHkey(key="/another/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    with pytest.raises(errors.AccessError):
        await facade.ddh_put(access, session, data)
    return


@pytest.mark.asyncio
async def test_set_consent_top(user, user2):
    """ test set consent at top """
    session = get_session(user)
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes={permissions.AccessMode.write})
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    await facade.ddh_put(access, session, consents.json())


@pytest.mark.asyncio
async def test_set_consent_deep(user, user2, user3):
    """ test set consent deeper in tree """
    session = get_session(user)
    # first set at top:
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes={permissions.AccessMode.write})
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    await facade.ddh_put(access, session, consents.json())

    # now withdraw access for user2 to a specific document, but give user3 access:
    ddhkey2 = keys.DDHkey(key="/mgf/org/private/documents:consents")
    access2 = permissions.Access(ddhkey=ddhkey, principal=user, modes={permissions.AccessMode.write})
    consents2 = permissions.Consents(consents=[permissions.Consent(grantedTo=[user3])])
    await facade.ddh_put(access2, session, consents2.json())


@pytest.mark.asyncio
async def test_write_data_with_consent(user, user2):
    """ test write through facade.ddh_put() with three objects:
        - mgf/.../doc1
        - another/.../doc2 with read grant to user
        - another/.../doc3
    """
    session = get_session(user)
    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)

    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc2")
    access = permissions.Access(ddhkey=ddhkey2, principal=user2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)
    # grant read access to user1
    consents = permissions.Consent.single(grantedTo=[user], withModes={permissions.AccessMode.read})
    ddhkey2f = ddhkey2.ensure_fork(keys.ForkType.consents)
    access = permissions.Access(ddhkey=ddhkey2f, principal=user2, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, consents.json())

    ddhkey3 = keys.DDHkey(key="/another/org/private/documents/doc3")
    access = permissions.Access(ddhkey=ddhkey3, principal=user2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much more'})
    await facade.ddh_put(access, session, data)

    return


@pytest.mark.asyncio
async def test_read_and_write_data(user, user2):
    session = get_session(user)
    # first, set up some data:
    await test_write_data_with_consent(user, user2)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()
    assert trx.read_consentees == transactions.DefaultReadConsentees

    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.read})
    await facade.ddh_get(access, session)

    # we have grant to read:
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc2")
    access = permissions.Access(ddhkey=ddhkey2, principal=user, modes={permissions.AccessMode.read})
    await facade.ddh_get(access, session)

    # we can write both docs to user
    ddhkeyW1 = keys.DDHkey(key="/mgf/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW1, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'no need to be related'})
    await facade.ddh_put(access, session, data)

    # but not to user2 as user:
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW2, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'no need to be related'})
    with pytest.raises(errors.AccessError):
        await facade.ddh_put(access, session, data)
    return


@pytest.mark.asyncio
async def test_read_and_write_data2(user, user2):
    session = get_session(user)
    # first, set up some data:
    await test_write_data_with_consent(user, user2)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction(for_user=user)

    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.read})
    await facade.ddh_get(access, session)
    assert principals.AllPrincipal.id not in trx.read_consentees, 'we have read object which does not have universal access'

    # we have grant to read:
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc2")
    access = permissions.Access(ddhkey=ddhkey2, principal=user2, modes={permissions.AccessMode.read})
    await facade.ddh_get(access, session)

    trx = await session.ensure_new_transaction(for_user=user2)

    # and not as user2 because we have existing object doc1 that user2 has no access to:
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    data = json.dumps({'document': 'no need to be related'})
    access = permissions.Access(ddhkey=ddhkeyW2, principal=user2, modes={permissions.AccessMode.write})
    with pytest.raises(transactions.TrxAccessError):
        await facade.ddh_put(access, session, data)

    # even with a new transaction
    await session.ensure_new_transaction()
    access = permissions.Access(ddhkey=ddhkeyW2, principal=user2, modes={permissions.AccessMode.write})
    with pytest.raises(transactions.TrxAccessError):
        await facade.ddh_put(access, session, data)

    # but with a reinit
    await session.reinit()
    await session.ensure_new_transaction(for_user=user2)
    access = permissions.Access(ddhkey=ddhkeyW2, principal=user2, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, data)
    return
