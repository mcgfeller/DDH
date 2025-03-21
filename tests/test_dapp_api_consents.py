""" Tests with Consents over Microservices """

import glom
import pytest
from core import keys, permissions


grant_key = keys.DDHkey(key="/mgf")
consent_key = grant_key.ensure_fork(keys.ForkType.consents)


@pytest.fixture(scope='module')
def put_consents(user1):
    """ Give consent on top key /mgf to lise """
    consents = permissions.Consents(consents=[permissions.Consent(grantedTo=['lise'])])
    r = user1.put('/ddh'+str(consent_key), json=consents.model_dump_json())
    r.raise_for_status()
    d = r.json()
    return d


def test_consents_received(user_lise, put_consents):
    """ read consents received for lise """
    _ = put_consents
    r = user_lise.get('/ddh/lise/org/ddh/consents/received')
    r.raise_for_status()
    d = r.json()
    g = d['grants']
    assert str(grant_key) in g
    assert glom.glom(g, f'{grant_key}.consents.0.grantedTo.0.id') == 'lise'
    return


def test_consents_given(user1, put_consents):
    """ read consents given by mgf """
    _ = put_consents
    r = user1.get('/ddh/mgf/org/ddh/consents/given')
    r.raise_for_status()
    d = r.json()
    g = d['grants']
    assert glom.glom(g, f'{grant_key}.consents.0.grantedTo.0.id') == 'lise'
    return
