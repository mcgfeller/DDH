""" Maintain user subscriptions to DApps. 
    TODO: Rewrite tu use Microservice to obtain DApps
"""
from core import keys,permissions,facade,errors,principals
from user import subscriptions
from frontend import user_auth,sessions
import pytest

@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')

@pytest.fixture(scope="module")
def session(user):
    return sessions.Session(token_str='test_session',user=user)

def test_subscription(user,session):
    """ Test adding a subscription """
    s = subscriptions.add_subscription(user.id,'MigrosDApp')
    l = subscriptions.list_subscriptions(user.id)
    return

def test_bad_app(user,session):
    """ Test adding a subscription """
    with pytest.raises(errors.NotFound):
        s = subscriptions.add_subscription(user.id,'LidlDApp')
    return


if __name__ == '__main__':
    test_subscription(user,session)