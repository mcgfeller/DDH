""" Tests Storage over DApp  """
import pytest
from tests import service_fixtures
from frontend import user_auth, sessions


@pytest.fixture(scope="session")
def storage_user1(httpx_processes):
    client = service_fixtures.get_authorized_client(httpx_processes, 'DApp.InMemStorage', {
                                                    'username': 'mgf', 'password': 'secret'}, tokenserver='api')
    yield client
    # Finalizer:
    client.close()
    return


@pytest.mark.asyncio
async def test_store_load_commit_data(storage_user1, httpx_processes):
    storage_user1.delete(f'/storage?confirm=True').raise_for_status()

    user = user_auth.UserInDB.load(storage_user1.headers['x-user'])
    session = sessions.Session(token_str=storage_user1.headers['authorization'], user=user)
    trx = await session.ensure_new_transaction()

    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.put(f'/storage/aaa?trxid={trx.trxid}', json='xxx')
    r.raise_for_status()
    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    r.raise_for_status()

    r = storage_user1.post(f'/transaction/{trx.trxid}/commit')
    r.raise_for_status()

    # new trx, must still exist since we have committed:

    trx = await session.ensure_new_transaction()
    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    r.raise_for_status()
    return


@pytest.mark.asyncio
async def test_store_load_abort_data(storage_user1, httpx_processes):
    storage_user1.delete(f'/storage?confirm=True').raise_for_status()

    user = user_auth.UserInDB.load(storage_user1.headers['x-user'])
    session = sessions.Session(token_str=storage_user1.headers['authorization'], user=user)
    trx = await session.ensure_new_transaction()

    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.put(f'/storage/aaa?trxid={trx.trxid}', json='xxx')
    r.raise_for_status()
    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    r.raise_for_status()

    r = storage_user1.post(f'/transaction/{trx.trxid}/abort')
    r.raise_for_status()

    # new trx, must not exist since we have aborted:

    trx = await session.ensure_new_transaction()
    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    assert r.status_code == 404
    return


@pytest.mark.asyncio
async def test_store_delete_commit_data(storage_user1, httpx_processes):
    storage_user1.delete(f'/storage?confirm=True').raise_for_status()

    user = user_auth.UserInDB.load(storage_user1.headers['x-user'])
    session = sessions.Session(token_str=storage_user1.headers['authorization'], user=user)
    trx = await session.ensure_new_transaction()

    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.put(f'/storage/aaa?trxid={trx.trxid}', json='xxx')
    r.raise_for_status()
    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    r.raise_for_status()

    r = storage_user1.delete(f'/storage/aaa?trxid={trx.trxid}')
    r.raise_for_status()

    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    assert r.status_code == 404, 'was deleted'

    r = storage_user1.post(f'/transaction/{trx.trxid}/commit')
    r.raise_for_status()

    trx = await session.ensure_new_transaction()
    r = storage_user1.post(f'/transaction/{trx.trxid}/begin')
    r.raise_for_status()

    r = storage_user1.get(f'/storage/aaa?trxid={trx.trxid}')
    assert r.status_code == 404, 'was deleted and committed'
    return
