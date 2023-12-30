""" Set up some Test data """

from core import keys, permissions, facade, errors, transactions, principals
from core import pillars
from traits import capabilities, anonymization, load_store
from frontend import user_auth, sessions
from backend import keyvault
import pytest
import json
from fastapi.encoders import jsonable_encoder

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
async def test_read_schema_top(user, transaction, migros_key_schema):

    session = get_session(user)
    trx = await session.ensure_new_transaction()
    # ddhkey1 = migros_key_schema[0].ensure_fork(keys.ForkType.schema)
    ddhkey1 = keys.DDHkeyVersioned0('//p:schema::0')
    access = permissions.Access(ddhkey=ddhkey1, principal=user)
    await facade.ddh_get(access, session)
    return


@pytest.mark.asyncio
async def test_write_schema_top_noaccess(user, transaction, migros_key_schema):

    session = get_session(user)
    trx = await session.ensure_new_transaction()
    ddhkey1 = keys.DDHkeyVersioned0('//p:schema::0')
    access = permissions.Access(ddhkey=ddhkey1, principal=user)
    with pytest.raises(errors.AccessError):
        s = await facade.ddh_put(access, session, {})
    return

# @pytest.mark.asyncio
# async def test_read_anon_failures(user, user2, no_storage_dapp):
#     """ Test must failures for anonymous access:
#         - must invoke anonymous if shared so
#         - private schema has no anonymous capability
#     """
#     session = get_session(user)
#     # first, set up some data:
#     await test_write_data_with_consent(user, user2, no_storage_dapp)
#     await session.reinit()  # ensure we have a clean slate
#     trx = await session.ensure_new_transaction()
#     assert trx.read_consentees == transactions.DefaultReadConsentees

#     ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc10")
#     access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={permissions.AccessMode.read})
#     await facade.ddh_get(access, session)

#     # read anonymous
#     access = permissions.Access(ddhkey=ddhkey1, principal=user, modes={
#                                 permissions.AccessMode.read, permissions.AccessMode.anonymous})
#     with pytest.raises(errors.CapabilityMissing):  # private schema does not have this capability
#         await facade.ddh_get(access, session)

#     # granted only with read anonymous
#     ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc20")
#     access = permissions.Access(ddhkey=ddhkey2, principal=user, modes={permissions.AccessMode.read})
#     with pytest.raises(errors.AccessError):  # we must supply AccessMode.anonymous, so this must raise AccessError
#         await facade.ddh_get(access, session)

#     access = permissions.Access(ddhkey=ddhkey2, principal=user, modes={
#                                 permissions.AccessMode.read, permissions.AccessMode.anonymous})

#     with pytest.raises(errors.CapabilityMissing):  # private schema does not have this capability
#         await facade.ddh_get(access, session)
#     return


# async def check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch, remainder=None) -> transactions.Transaction:

#     async def monkey_apply0(*a, **kw):
#         """ load_store.LoadFromStorage """
#         return None

#     async def monkey_apply1(*a, **kw):
#         """ load_store.LoadFromDApp """
#         a[2].parsed_data = m_data
#         return

#     monkeypatch.setattr(load_store.LoadFromStorage, 'apply', monkey_apply0)
#     monkeypatch.setattr(load_store.LoadFromDApp, 'apply', monkey_apply1)

#     session = get_session(user)
#     await session.reinit()  # ensure we have a clean slate
#     trx = await session.ensure_new_transaction()
#     assert trx.read_consentees == transactions.DefaultReadConsentees

#     k, schema = migros_key_schema
#     schema = schema.to_json_schema()
#     migros_key_schema = (k, schema)

#     if remainder:
#         k = k + remainder
#         m_data = {user.id: migros_data[user.id][remainder]}
#     else:
#         m_data = migros_data
#     # data is obtained from DApp via JSON, so convert to JSON and load again
#     m_data = json.loads(json.dumps(jsonable_encoder(m_data)))
#     # read anonymous
#     access = permissions.Access(ddhkey=k.ensure_fork(keys.ForkType.data), principal=user, modes=modes)
#     cumulus = migros_data[user.id]['cumulus']
#     access.schema_key_split = 4  # split after the migros.org
#     trargs = await schema.apply_transformers(access, trx, None)  # transformer processing happens here
#     data = trargs.parsed_data
#     assert user.id not in data, 'eid must be anonymized'
#     assert len(data) == 1, 'one user only'
#     d = list(data.values())[0]
#     if not remainder:
#         assert cumulus != d['cumulus'], 'qid must be anonymized'
#         receipts = d['receipts']
#     else:
#         receipts = d
#     assert not any(rec['Filiale'].startswith('MM ') for rec in receipts), 'sa Filiale must be anonymized'
#     return trargs


# @pytest.mark.asyncio
# async def test_read_anon_migros(user, transaction, migros_key_schema, migros_data, monkeypatch):
#     """ read anonymous whole schema """
#     modes = {permissions.AccessMode.read, permissions.AccessMode.anonymous}
#     await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
#     return


# @pytest.mark.asyncio
# async def test_read_pseudo_migros(user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
#     """ read pseudonymous whole schema """
#     modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
#     trargs = await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
#     eid = list(trargs.parsed_data.keys())[0]
#     pm = await anonymization.PseudonymMap.load(eid, user, transaction)  # retrieve it
#     assert isinstance(pm, anonymization.PseudonymMap)
#     assert isinstance(pm.inverted_cache, dict)
#     assert pm.inverted_cache[('', '', eid)] == user.id  # map back to user

#     return


# @pytest.mark.asyncio
# async def test_read_write_pseudo_migros(user, transaction, migros_key_schema, migros_data, monkeypatch, no_storage_dapp):
#     """ read pseudonymous whole schema """
#     modes = {permissions.AccessMode.read, permissions.AccessMode.pseudonym}
#     trargs = await check_data_with_mode(user, transaction, migros_key_schema, migros_data, modes, monkeypatch)
#     schema = trargs.nschema  # modified in check_data_with_mode
#     k = migros_key_schema[0]
#     eid = list(trargs.parsed_data.keys())[0]

#     data = trargs.parsed_data[eid]  # without owner for writing
#     data = json.dumps(jsonable_encoder(data))  # back to json
#     modes = {permissions.AccessMode.write, permissions.AccessMode.pseudonym}
#     ddhkey = k.ensure_fork(keys.ForkType.data).with_new_owner(eid)
#     access = permissions.Access(ddhkey=ddhkey, principal=user, modes=modes)
#     access.schema_key_split = 4  # split after the migros.org
#     trargs = await schema.apply_transformers(access, transaction, data)  # transformer processing happens here
#     data = trargs.parsed_data
#     return