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
async def test_read_anon(user, user2):
    session = get_session(user)
    # first, set up some data:
    await test_write_data_with_consent(user, user2)
    session.reinit()  # ensure we have a clean slate
    trx = session.new_transaction()
    assert trx.read_consentees == transactions.DefaultReadConsentees

    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc10")
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.read})
    await facade.ddh_get(access, session)

    # read anonymous
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={
                                permissions.AccessMode.read, permissions.AccessMode.anonymous})
    with pytest.raises(errors.CapabilityMissing):
        await facade.ddh_get(access, session)

    # granted only with read anonymous
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc20")
    access = permissions.Access(ddhkey=ddhkey2, principal=user, modes={permissions.AccessMode.read})
    with pytest.raises(errors.AccessError):  # we must supply AccessMode.anonymous, so this must raise AccessError
        await facade.ddh_get(access, session)

    access = permissions.Access(ddhkey=ddhkey2, principal=user, modes={
                                permissions.AccessMode.read, permissions.AccessMode.anonymous})
    with pytest.raises(errors.CapabilityMissing):  # TODO Once we have the capable Schema, this must not raise error
        await facade.ddh_get(access, session)
    return


@pytest.mark.asyncio
async def test_write_data_with_consent(user, user2):
    """ test write through facade.ddh_put() with three objects:
        - mgf/.../doc1
        - another/.../doc2 with read grant to user
        - another/.../doc3
    """
    session = get_session(user)
    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc10")
    access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)

    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc20")
    access = permissions.Access(ddhkey=ddhkey2, principal=user2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)
    # grant anonymous read access to user1
    consents = permissions.Consent.single(grantedTo=[user], withModes={
                                          permissions.AccessMode.read, permissions.AccessMode.anonymous})
    ddhkey2f = ddhkey2.ensure_fork(keys.ForkType.consents)
    access = permissions.Access(ddhkey=ddhkey2f, principal=user2, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, consents.json())

    # TODO: We need to put a schema here that supports the Anonymous capability:
    ddhkey2s = ddhkey2.ensure_fork(keys.ForkType.schema)

    ddhkey3 = keys.DDHkey(key="/another/org/private/documents/doc30")
    access = permissions.Access(ddhkey=ddhkey3, principal=user2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much more'})
    await facade.ddh_put(access, session, data)

    return
