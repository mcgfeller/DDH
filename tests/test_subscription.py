""" Maintain user subscriptions to DApps. 
    Use Microservice to obtain DApps
"""
from core import errors
import pytest


def test_subscription(user1_sub):
    """ Test adding a subscription """
    user = user1_sub.headers['x-user']
    r = user1_sub.post('/users/{user}/subscriptions/dapp/MigrosDApp')
    r.raise_for_status()
    d = r.json()
    assert 'MigrosDApp' in d
    return

def test_bad_app(user1_sub):
    """ Test adding a subscription """
    user = user1_sub.headers['x-user']
    r = user1_sub.post('/users/{user}/subscriptions/dapp/UnknownDApp')
    assert r.status_code == 404
    return


if __name__ == '__main__':
    test_subscription(user1_sub)