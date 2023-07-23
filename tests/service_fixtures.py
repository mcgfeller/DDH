""" Fixtures for testing with actual microservices """


import httpx
import pytest
import pcp
import time


@pytest.fixture(scope="session")
def httpx_processes(wait: float = 3):
    """ Start the uvicorn server with the FastAPI app on PORT;
        Finalizer terminated started server.
    """
    processes = start_servers()
    time.sleep(wait)  # give a bit time to start
    yield processes
    # Finalizer:
    processes.stop(pcp.getargs())
    return


@pytest.fixture(scope="session")
def user1(httpx_processes):
    client = get_authorized_client(httpx_processes, 'api', {'username': 'mgf', 'password': 'secret'})
    yield client
    # Finalizer:
    client.close()
    return


@pytest.fixture(scope="session")
def user1_sub(httpx_processes):
    client = get_authorized_client(httpx_processes, 'subscription', {'username': 'mgf', 'password': 'secret'})
    yield client
    # Finalizer:
    client.close()
    return


@pytest.fixture(scope="session")
def user1_market(httpx_processes):
    client = get_authorized_client(httpx_processes, 'market', {'username': 'mgf', 'password': 'secret'})
    yield client
    # Finalizer:
    client.close()
    return


def get_authorized_client(processes, procid, userpwd, tokenserver: str | None = None, add_headers: dict = {}) -> httpx.Client:
    """ return a client with header configured for userpwd """
    port = processes.get(procid)[0].port  # get the API server
    tokenurl = url = 'http://localhost:'+str(port)
    if tokenserver and tokenserver != procid:
        tokenport = processes.get(tokenserver)[0].port  # get the token API server
        tokenurl = 'http://localhost:'+str(tokenport)  # get the API server

    r = httpx.post(tokenurl+'/token', data=userpwd)  # obtain token
    r.raise_for_status()
    token = r.json()['access_token']
    headers = httpx.Headers({'Authorization': 'Bearer '+token, 'x-user': userpwd['username']})
    headers.update(add_headers)
    client = httpx.Client(base_url=url, headers=headers)  # client with token in header
    return client


def start_servers():
    """ Start all DDH servers, including DApps """
    pcp.ddh.start(pcp.getargs())
    return pcp.ddh
