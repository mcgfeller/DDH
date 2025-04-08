""" Tests with Events over Microservices """

import pytest
from core import keys, permissions
import json


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


def test_events_subscriptions(user1):
    """ subscribe for events 
        TODO:#35
    """
    j = {'subscriptions': [{'key': '/mgf/org/ddh/consents/received'}, {'key': '/mgf/org/private/documents'},
                           {'key': '/mgf/p/living/shopping/receipts'}]}
    r = user1.put('/ddh/mgf/org/ddh/events/subscriptions', json=json.dumps(j))
    r.raise_for_status()
    d = r.json()

    r = user1.get('/ddh/mgf/org/ddh/events/subscriptions')
    r.raise_for_status()
    d = r.json()
    return


def test_events_wait(user1):
    """ read next event
        TODO:#35
    """
    test_events_subscriptions(user1)
    # write something to create an event:
    j = {'data': 'test_event_wait'}
    r = user1.put('/ddh/mgf/org/private/documents/doc1', json=json.dumps(j))
    r.raise_for_status()
    # wait for event:
    r = user1.get('/ddh/mgf/org/ddh/events/wait/mgf/org/private/documents?nowait=True', timeout=2)
    r.raise_for_status()
    d = r.json()
    return
