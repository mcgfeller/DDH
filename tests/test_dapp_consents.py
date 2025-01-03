""" Tests with Consents over Microservices """

import pytest
from core import keys, permissions


@pytest.fixture(scope='module')
def put_consents(user1):
    """ Give consent on top key /mgf to lise """
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey, modes={permissions.AccessMode.write})
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=['lise'])])
    r = user1.put('/ddh'+str(ddhkey), json=consents.model_dump_json())
    r.raise_for_status()
    d = r.json()
    return d


def test_consents_received(user_lise, put_consents):
    """ read consents received for lise """
    _ = put_consents
    r = user_lise.get('/ddh/lise/org/ddh/consents/received')
    r.raise_for_status()
    d = r.json()
    return


def test_consents_given(user1, put_consents):
    """ read consents given by mgf """
    _ = put_consents
    r = user1.get('/ddh/mgf/org/ddh/consents/given')
    r.raise_for_status()
    d = r.json()
    return
