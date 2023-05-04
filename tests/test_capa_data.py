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


async def check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, remainder=None) -> transactions.Transaction:

    session = get_session(user)
    session.reinit()  # ensure we have a clean slate
    trx = session.new_transaction()
    assert trx.read_consentees == transactions.DefaultReadConsentees

    k, schema = migros_key_schema
    if remainder:
        k = k + remainder
        m_data = {user.id: migros_data[user.id][remainder]}
    else:
        m_data = migros_data
    # read anonymous
    access = permissions.Access(ddhkey=k.ensure_fork(keys.ForkType.data), principal=user, modes=modes)
    # TODO: Consider mocking data access - fow now, call after_data_read with data directly instead
    # data = await facade.ddh_get(access, session)
    cumulus = migros_data[user.id]['cumulus']
    access.schema_key_split = 4
    data = schema.after_data_read(access, trx, m_data)
    assert user.id not in data, 'eid must be anonymized'
    d = list(data.values())[0]
    if not remainder:
        assert cumulus != d['cumulus'], 'qid must be anonymized'
        receipts = d['receipts']
    else:
        receipts = d
    assert not any(rec['Filiale'].startswith('MM ') for rec in receipts), 'sa Filiale must be anonymized'
    return trx


@pytest.mark.asyncio
async def test_read_anon_migros(user, transaction, migros_key_schema, migros_data):
    """ read anonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes)
    return


@pytest.mark.asyncio
async def test_read_pseudo_migros(user, transaction, migros_key_schema, migros_data):
    """ read pseudonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
    trx = await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes)
    assert trx.actions
    assert trx.actions[0].obj.__class__.__name__ == 'PseudonymMap'
    return


@pytest.mark.asyncio
async def test_read_anon_migros_rec(user, transaction, migros_key_schema, migros_data):
    """ read anononymous within schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, remainder='receipts')
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
