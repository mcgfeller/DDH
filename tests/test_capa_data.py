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
    return data, header


async def grant_consents(ddhkey: keys.DDHkey | str, consents: permissions.Consents, no_storage_dapp=None):
    """ utility to grant consents to ddhkey
    """
    if isinstance(ddhkey, str):
        ddhkey = keys.DDHkey(ddhkey)
    user = user_auth.UserInDB.load(ddhkey.owner)
    session = get_session(user)
    ddhkey_c = ddhkey.ensure_fork(keys.ForkType.consents)
    access = permissions.Access(ddhkey=ddhkey_c, modes={permissions.AccessMode.write})
    await facade.ddh_put(access, session, consents.model_dump_json())
    return


async def grant_consents_to_migros(migros_key_schema, user2, user, modes: set[permissions.AccessMode] = {permissions.AccessMode.read, permissions.AccessMode.anonymous}, no_storage_dapp=None):
    ddhkey = migros_key_schema[0].ensure_fork(keys.ForkType.data).with_new_owner(user.id)
    await grant_consents(ddhkey, permissions.Consent.single(grantedTo=[user2], withModes=modes))


@pytest.mark.asyncio
async def test_read_anon_failures(user, user2, no_storage_dapp):
    """ Test expected failures for anonymous access:
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


async def check_data_with_mode(access_user, transaction, migros_key_schema, migros_data, modes, monkeypatch, anon_owner_id: str = None, remainder=None) -> transactions.Transaction:

    async def monkey_apply1(*a, **kw):
        """ load_store.LoadFromDApp, load from memory """
        a[2].parsed_data = m_data
        return
    data_user_id = 'mgf'

    monkeypatch.setattr(load_store.LoadFromDApp, 'apply', monkey_apply1)

    session = get_session(access_user)
    await session.reinit()  # ensure we have a clean slate
    trx = await session.ensure_new_transaction()
    assert trx.read_consentees == set()

    k, schema = migros_key_schema
    schema = schema.to_json_schema()
    migros_key_schema = (k, schema)

    if remainder:
        k = k + remainder
        m_data = {data_user_id: migros_data[data_user_id][remainder]}
    else:
        m_data = migros_data
    # data is obtained from DApp via JSON, so convert to JSON and load again
    m_data = json.loads(json.dumps(jsonable_encoder(m_data)))
    # read anonymous
    anon_owner_id = anon_owner_id or data_user_id
    ddhkey = k.ensure_fork(keys.ForkType.data).with_new_owner(anon_owner_id)
    access = permissions.Access(ddhkey=ddhkey, modes=modes)
    cumulus = migros_data[data_user_id]['cumulus']
    access.schema_key_split = 4  # split after the migros.org
    trx.add_and_validate(access)
    trstate = await schema.apply_transformers(access, trx, None, {})  # transformer processing happens here
    data = trstate.parsed_data
    assert data_user_id not in data, 'eid must be anonymized'
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
async def test_read_anon_migros(user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read anonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
    return


@pytest.mark.asyncio
async def test_read_anon_migros_without_grant(user3, user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read anonymous whole schema, no grant given to user3 """
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    with pytest.raises(errors.AccessError):
        await check_data_with_mode(user3, transaction, migros_key_schema, migros_data, modes, monkeypatch)
    return


def anon_key(key: keys.DDHkey, grants) -> list[keys.DDHkey]:
    """ return a list of  data keys with anonymized owner to fetch key
        grants isreturned by /{user2.id}/org/ddh/consents/received
    """
    match_key = key.without_variant_version().ensure_fork(keys.ForkType.data)
    matched = []
    for g in grants.grants.keys():
        k = keys.DDHkey(g)
        if k.without_owner() == match_key:
            matched.append(match_key.with_new_owner(k.owner))
    return matched


async def get_grants_received(user, migros_key_schema, session) -> keys.DDHkey:
    d, h = await read(f"/{user.id}/org/ddh/consents/received", session)
    key = anon_key(migros_key_schema[0], d)
    # assert len(key) == 1
    return key[0]


@pytest.mark.asyncio
async def test_read_anon_migros_with_grant(user2, user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read anonymous whole schema, with grant given to user2  """
    # Give Grant to user2:
    await grant_consents_to_migros(migros_key_schema, user2, user)
    # get grants received:
    session = get_session(user2)  # get trx corresponding to user
    transaction = session.get_or_create_transaction()
    key = await get_grants_received(user2, migros_key_schema, session)
    # now we can read
    modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
    trstate = await check_data_with_mode(user2, transaction, migros_key_schema, migros_data, modes, monkeypatch, anon_owner_id=key.owner)
    eid = list(trstate.parsed_data.keys())[0]
    assert eid == key.owner
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
async def test_read_pseudo_migros_with_grant(user2, user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read pseudonymous whole schema by user2 """
    modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
    # Give Grant to user2:
    await grant_consents_to_migros(migros_key_schema, user2, user, modes=modes)
    # get grants received:
    session = get_session(user2)  # get trx corresponding to user
    transaction = session.get_or_create_transaction()
    key = await get_grants_received(user2, migros_key_schema, session)

    trstate = await check_data_with_mode(user2, transaction, migros_key_schema, migros_data, modes, monkeypatch, anon_owner_id=key.owner)
    eid = list(trstate.parsed_data.keys())[0]
    assert eid == key.owner
    pm = await anonymization.PseudonymMap.load(eid, user2, transaction)  # retrieve it
    assert isinstance(pm, anonymization.PseudonymMap)
    assert isinstance(pm.inverted_cache, dict)
    assert pm.inverted_cache[('', '', eid)] == user.id  # map back to user

    return


@pytest.mark.asyncio
async def test_read_write_pseudo_migros(user2, user, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
    """ read pseudonymous whole schema """
    modes = {permissions.AccessMode.read, permissions.AccessMode.write, permissions.AccessMode.pseudonym}
    # Give Grant to user2:
    await grant_consents_to_migros(migros_key_schema, user2, user, modes=modes)
    session = get_session(user2)  # get trx corresponding to user
    transaction = session.get_or_create_transaction()
    key = await get_grants_received(user2, migros_key_schema, session)

    modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
    trstate = await check_data_with_mode(user2, transaction, migros_key_schema, migros_data, modes, monkeypatch, anon_owner_id=key.owner)
    schema = trstate.nschema  # modified in check_data_with_mode
    k = migros_key_schema[0]
    eid = list(trstate.parsed_data.keys())[0]
    assert eid == key.owner

    data = trstate.parsed_data[eid]  # without owner for writing
    data = json.dumps(jsonable_encoder(data))  # back to json
    modes = {permissions.AccessMode.write, permissions.AccessMode.pseudonym}
    ddhkey = k.ensure_fork(keys.ForkType.data).with_new_owner(eid)
    access = permissions.Access(ddhkey=ddhkey, principal=user2, modes=modes, op=permissions.Operation.put)
    access.schema_key_split = 4  # split after the migros.org
    trstate = await schema.apply_transformers(access, transaction, data, {})  # transformer processing happens here
    data = trstate.parsed_data
    return


@pytest.mark.asyncio
async def test_read_anon_migros_rec(user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
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
