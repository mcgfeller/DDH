""" Test actual FastAPI Server """

import pytest
import httpx
import subprocess
import pathlib


UVICORN_EXE = "C:\\Program Files\\Python38\\scripts\\uvicorn.exe"
PORT = 8048 
USERPWD = {'username':'mgf','password':'secret'}

def test_get_data(httpx_client):
    r = httpx_client.get('/data/ddh/shopping/stores/migros/clients/receipts')
    r.raise_for_status()
    d = r.json()
    assert d['res'],'res is empty'
    return

def test_get_schema(httpx_client):
    r = httpx_client.get('/schema/ddh/shopping?schemaformat=json')
    r.raise_for_status()
    r.json()
    return


@pytest.fixture(scope="module")
def httpx_client():
    """ Start the uvicorn server with the FastAPI app on PORT;
        Finalizer terminated started server.
    """
    process = start_server(exe=UVICORN_EXE,port=PORT)
    url = 'http://localhost:'+str(PORT)
    r = httpx.post(url+'/token',data=USERPWD)
    r.raise_for_status()
    token = r.json()['access_token']
    headers = httpx.Headers({'Authorization': 'Bearer '+token})
    client = httpx.Client(base_url=url,headers=headers)
    yield client
    # Finalizer:
    client.close()
    process.terminate()
    return 


def start_server(exe : str,port : int = 8080, app : str = 'main:app',cwd=pathlib.Path(__file__).parent.parent) -> subprocess.Popen:
    p = subprocess.Popen([exe,app,f'--port={port}','--reload' ],bufsize=-1,cwd=cwd)
    return p