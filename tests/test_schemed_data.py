""" Test get and putting data, against schema """
import pytest
import httpx
import copy


def test_get_and_putdata(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'omit_owner': False})
    r.raise_for_status()


def test_get_and_putdata_nonexist(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    with pytest.raises(httpx.HTTPStatusError):
        r = user1.put('/ddh/mgf/bad', json=data, params={'omit_owner': False})
        t = r.json()['detail']
        r.raise_for_status()


def test_get_and_putdata_validation_errors(user1):
    """ get data and rewrite it to same place with same user, but with validation errors """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    data['mgf']['bad'] = {'bla': 'foo'}
    with pytest.raises(httpx.HTTPStatusError):
        r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'omit_owner': False})
        t = r.json()['detail']
        assert "'bad' was unexpected" in t
        r.raise_for_status()

    data['mgf'].pop('bad')
    data['mgf']['receipts'][0]['Kassennummer'] = 436.5  # float is not  alloed
    with pytest.raises(httpx.HTTPStatusError):
        r = user1.put('/ddh/mgf/org/migros.ch', json=data, params={'omit_owner': False})
        t = r.json()['detail']
        assert "is not of type 'integer'" in t
        r.raise_for_status()


def test_get_and_putdata_oldversion(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    with pytest.raises(httpx.HTTPStatusError):
        r = user1.put('/ddh/mgf/org/migros.ch:::0.1', json=data, params={'omit_owner': False})
        t = r.json()['detail']
        r.raise_for_status()
