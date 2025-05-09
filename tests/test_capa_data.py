""" Set up some Test data """

from core import keys, permissions, facade, errors, transactions
from core import pillars
from traits import capabilities, anonymization, load_store
from frontend import user_auth, sessions
from backend import keyvault
import pytest
import json
from fastapi.encoders import jsonable_encoder

keyvault.clear_vaults()  # need to be independent of other tests


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


async def read(ddhkey: keys.DDHkey | str, session: sessions.Session, modes: set[permissions.AccessMode] = {permissions.AccessMode.read}):
    if isinstance(ddhkey, str):
        ddhkey = keys.DDHkey(ddhkey)
    access = permissions.Access(ddhkey=ddhkey, modes=modes)
    data, header = await facade.ddh_get(access, session)
    assert data, 'data must not be empty'
    return


@pytest.mark.asyncio
async def test_read_anon_failures(user, user2, no_storage_dapp):
    """ Test must failures for anonymous access:
        - must invoke anonymous if shared so
        - private schema has no anonymous capability
    """
    session = get_session(user)
    # first, set up some data:
    await test_write_data_with_consent(user, user2, no_storage_dapp)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()
    assert trx.read_consentees == set()

    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc10")
    # access = permissions.Access(ddhkey=ddhkey1, modes={permissions.AccessMode.read})
    # await facade.ddh_get(access, session)
    await read("/mgf/org/private/documents/doc10", session, modes={permissions.AccessMode.read})

    # read anonymous
    with pytest.raises(errors.CapabilityMissing):  # private schema does not have this capability
        await read("/mgf/org/private/documents/doc10", session, modes={permissions.AccessMode.read, permissions.AccessMode.anonymous})

    # granted only with read anonymous
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc20")
    with pytest.raises(errors.AccessError):  # we must supply AccessMode.anonymous, so this must raise AccessError
        await read(ddhkey2, session, modes={permissions.AccessMode.read, })

    with pytest.raises(errors.CapabilityMissing):  # private schema does not have this capability
        await read(ddhkey2, session, modes={permissions.AccessMode.read, permissions.AccessMode.anonymous})
    return


async def check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch, remainder=None) -> transactions.Transaction:

    async def monkey_apply0(*a, **kw):
        """ load_store.LoadFromStorage """
        return None

    async def monkey_apply1(*a, **kw):
        """ load_store.LoadFromDApp """
        a[2].parsed_data = m_data
        return

    monkeypatch.setattr(load_store.LoadFromStorage, 'apply', monkey_apply0)
    monkeypatch.setattr(load_store.LoadFromDApp, 'apply', monkey_apply1)

    session = get_session(user)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()
    assert trx.read_consentees == set()

    k, schema = migros_key_schema
    schema = schema.to_json_schema()
    migros_key_schema = (k, schema)

    if remainder:
        k = k + remainder
        m_data = {user.id: migros_data[user.id][remainder]}
    else:
        m_data = migros_data
    # data is obtained from DApp via JSON, so convert to JSON and load again
    m_data = json.loads(json.dumps(jsonable_encoder(m_data)))
    # read anonymous
    access = permissions.Access(ddhkey=k.ensure_fork(keys.ForkType.data), modes=modes)
    cumulus = migros_data[user.id]['cumulus']
    access.schema_key_split = 4  # split after the migros.org
    trstate = await schema.apply_transformers(access, trx, None, {})  # transformer processing happens here
    data = trstate.parsed_data
    assert user.id not in data, 'eid must be anonymized'
    assert len(data) == 1, 'one user only'
    d = list(data.values())[0]
    if not remainder:
        assert cumulus != d['cumulus'], 'qid must be anonymized'
        receipts = d['receipts']
    else:
        receipts = d
    assert not any(rec['Filiale'].startswith('MM ') for rec in receipts), 'sa Filiale must be anonymized'
    return trstate


@pytest.mark.asyncio
async def test_read_anon_migros(user, transaction, migros_key_schema, migros_data, monkeypatch):
    """ read anonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
    return


@pytest.mark.asyncio
async def test_read_pseudo_migros(user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read pseudonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
    trstate = await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
    eid = list(trstate.parsed_data.keys())[0]
    pm = await anonymization.PseudonymMap.load(eid, user, transaction)  # retrieve it
    assert isinstance(pm, anonymization.PseudonymMap)
    assert isinstance(pm.inverted_cache, dict)
    assert pm.inverted_cache[('', '', eid)] == user.id  # map back to user

    return


@pytest.mark.asyncio
async def test_read_write_pseudo_migros(user, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read pseudonymous whole schema """
    session = get_session(user)  # get trx corresponding to user
    transaction = session.get_or_create_transaction()
    modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
    trstate = await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
    schema = trstate.nschema  # modified in check_data_with_mode
    k = migros_key_schema[0]
    eid = list(trstate.parsed_data.keys())[0]

    data = trstate.parsed_data[eid]  # without owner for writing
    data = json.dumps(jsonable_encoder(data))  # back to json
    modes = {permissions.AccessMode.write, permissions.AccessMode.pseudonym}
    ddhkey = k.ensure_fork(keys.ForkType.data).with_new_owner(eid)
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes=modes, op=permissions.Operation.put)
    access.schema_key_split = 4  # split after the migros.org
    trstate = await schema.apply_transformers(access, transaction, data, {})  # transformer processing happens here
    data = trstate.parsed_data
    return


@pytest.mark.asyncio
async def test_read_anon_migros_rec(user, transaction, migros_key_schema, migros_data, monkeypatch):
    """ read anononymous within schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch, remainder='receipts')
    return


@pytest.mark.asyncio
async def test_write_data_with_consent(user, user2, no_storage_dapp):
    """ test write through facade.ddh_put() with three objects:
        - mgf/.../doc1
        - another/.../doc2 with read grant to user
        - another/.../doc3
    """
    session = get_session(user)
    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc10")
    access = permissions.Access(ddhkey=ddhkey1, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session, data)

    session2 = get_session(user2)
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc20")
    access = permissions.Access(ddhkey=ddhkey2, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much'})
    await facade.ddh_put(access, session2, data)
    # grant anonymous read access to user1
    consents = permissions.Consent.single(grantedTo=[user], withModes={
                                          permissions.AccessMode.read, permissions.AccessMode.anonymous})
    ddhkey2_c = ddhkey2.ensure_fork(keys.ForkType.consents)
    access = permissions.Access(ddhkey=ddhkey2_c, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session2, consents.model_dump_json())

    # TODO: We need to put a schema here that supports the Anonymous capability:
    ddhkey2_s = ddhkey2.ensure_fork(keys.ForkType.schema)

    ddhkey3 = keys.DDHkey(key="/another/org/private/documents/doc30")
    access = permissions.Access(ddhkey=ddhkey3, modes={permissions.AccessMode.write})
    data = json.dumps({'document': 'not much more'})
    await facade.ddh_put(access, session2, data)

    return
