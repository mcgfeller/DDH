""" Set up some Test data """
from core import keys,permissions,facade,errors,principals
from core import pillars
from frontend import user_auth,sessions
from market import recommender
import pytest

@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')

@pytest.fixture(scope="module")
def session(user):
    return sessions.Session(token_str='test_session',user=user)

def test_query(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    dapps = recommender.search_dapps(session,query='tax')
    assert dapps
    return


if __name__ == '__main__':
    test_query(user,session)