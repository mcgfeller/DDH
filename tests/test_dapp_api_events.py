""" Tests with Events over Microservices """

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


def test_events_subscribe(user1):
    """ subscribe for events 
        TODO:#35
    """
    j = {}
    r = user1.put('/ddh/mgf/org/ddh/events/subscribe', json=j)
    r.raise_for_status()
    d = r.json()
    return


def test_events_wait(user1):
    """ read next event
        TODO:#35
    """
    r = user1.get('/ddh/mgf/org/ddh/events/wait')
    r.raise_for_status()
    d = r.json()
    return
