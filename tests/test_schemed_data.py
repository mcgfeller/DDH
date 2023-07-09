""" Test get and putting data, against schema """
import pytest
import httpx
import copy


def test_get_and_putdata(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'includes_owner': True})
    r.raise_for_status()


def test_get_and_putdata_std(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/p/living/shopping/receipts')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/p/living/shopping/receipts', json=data, params={'includes_owner': True})
    r.raise_for_status()


def test_get_and_putdata_nonexist(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/bad', json=data, params={'includes_owner': True})
    assert r.status_code == 404
    t = r.json().get('detail')
    assert 'is not in schema' in t


def test_get_and_putdata_validation_errors(user1):
    """ get data and rewrite it to same place with same user, but with validation errors """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()

    data['mgf']['bad'] = {'bla': 'foo'}
    r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'includes_owner': True})
    assert r.status_code == 422
    t = r.json().get('detail')
    assert "'bad' was unexpected" in t

    data['mgf'].pop('bad')
    data['mgf']['receipts'][0]['Kassennummer'] = 436.5  # float is not  allowed

    r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'includes_owner': True})
    assert r.status_code == 422
    t = r.json().get('detail')
    assert "is not of type 'integer'" in t
    return


def test_get_and_putdata_oldversion(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/org/migros.ch:::0.1', json=data, params={'includes_owner': True})
    assert r.status_code == 422
    t = r.json().get('detail')
    assert "is not latest version" in t
    return
