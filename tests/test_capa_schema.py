""" Set up some Test data """

from core import keys, permissions, facade, errors, transactions, principals, users
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


@pytest.fixture(scope="module")
def migros_user():
    """ User with write access to migros.ch """
    return users.User(id='migros', name='Migros schema owner')


@pytest.fixture(scope="module")
def coop_user():
    """ User with write access to coop.ch """
    return users.User(id='coop', name='Coop schema owner')


@pytest.fixture(scope="module")
def migros_key_schema_json(migros_key_schema):
    key = migros_key_schema[0].ensure_fork(keys.ForkType.schema)
    data = migros_key_schema[1].to_json_schema().model_dump_json()
    return (key, data)


@pytest.fixture(scope="module")
def coop_key_schema_json(migros_key_schema):
    """ for now, we just fake it and use same as Migros - sorry, children on Coop """
    key = keys.DDHkeyVersioned0('//org/coop.ch:schema::0.2')
    data = migros_key_schema[1].to_json_schema().model_dump_json()
    return (key, data)


def get_session(user):
    return sessions.Session(token_str='test_session_'+user.id, user=user)


@pytest.mark.asyncio
async def test_read_schema_p(user, transaction, migros_key_schema):

    session = get_session(user)
    trx = await session.ensure_new_transaction()
    ddhkey1 = keys.DDHkeyVersioned0('//p:schema::0')
    access = permissions.Access(ddhkey=ddhkey1)
    s = await facade.ddh_get(access, session)
    return


@pytest.mark.asyncio
async def test_read_schema_org(user, transaction, migros_key_schema):
    """ Read integrared schema //org: """
    session = get_session(user)
    trx = await session.ensure_new_transaction()
    ddhkey1 = keys.DDHkeyVersioned0('//org:schema::0')
    access = permissions.Access(ddhkey=ddhkey1)
    s = await facade.ddh_get(access, session)
    return


@pytest.mark.asyncio
async def test_put_schema_migros(migros_user, migros_key_schema_json):
    """ put the migros schema with authorized user """
    session = get_session(migros_user)
    trx = await session.ensure_new_transaction()
    access = permissions.Access(ddhkey=migros_key_schema_json[0])
    await facade.ddh_put(access, session, migros_key_schema_json[1])
    return


@pytest.mark.asyncio
async def test_put_schema_migros_bad_user(user, migros_key_schema_json):
    """ put schema with user who is not authorized """
    session = get_session(user)
    trx = await session.ensure_new_transaction()
    access = permissions.Access(ddhkey=migros_key_schema_json[0])
    with pytest.raises(errors.AccessError):
        await facade.ddh_put(access, session, migros_key_schema_json[1])
    return


@pytest.mark.asyncio
async def test_put_schema_coop(coop_user, coop_key_schema_json):
    """ put the coop schema (which doesn't exist) with authorized user """
    session = get_session(coop_user)
    trx = await session.ensure_new_transaction()
    access = permissions.Access(ddhkey=coop_key_schema_json[0])
    await facade.ddh_put(access, session, coop_key_schema_json[1])
    return


@pytest.mark.asyncio
async def test_write_schema_top_noaccess(user, transaction, migros_key_schema):

    session = get_session(user)
    trx = await session.ensure_new_transaction()
    ddhkey1 = keys.DDHkeyVersioned0('//p:schema::0')
    access = permissions.Access(ddhkey=ddhkey1)
    with pytest.raises(errors.AccessError):
        s = await facade.ddh_put(access, session, {})
    return
