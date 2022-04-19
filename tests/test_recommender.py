""" Set up some Test data """
from core import keys,permissions,facade,errors,principals,common_ids
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

def test_all(user,session):
    """ Test withhout criteria """
    sris = recommender.search_dapps(session,query=None,categories=None,desired_labels=None)
    assert len(sris) == len(pillars.DAppManager.DAppsById)
    return

def test_query_1(user,session):
    """ Test simple text query """
    sris = recommender.search_dapps(session,query='tax',categories=None,desired_labels=None)
    assert sris
    return

def test_query_label_1(user,session):
    """ Simple text + fulfilled label """
    sris = recommender.search_dapps(session,query='tax',categories=None,desired_labels=[common_ids.Label.free])
    assert sris
    return

def test_query_label_0(user,session):
    """  Simple text + non-fulfilled label """
    sris = recommender.search_dapps(session,query='tax',categories=None,desired_labels=[common_ids.Label.anonymous])
    assert sris
    assert sris[0].merit < 0
    assert common_ids.Label.anonymous in sris[0].ignored_labels
    return

def test_query_cat_1(user,session):
    """ Test simple text query """
    sris = recommender.search_dapps(session,query='tax',categories=[common_ids.CatalogCategory.finance],desired_labels=None)
    assert sris
    return

def test_query_cat_label_1(user,session):
    """ Test simple text query """
    sris = recommender.search_dapps(session,query='tax',categories=[common_ids.CatalogCategory.finance],desired_labels=[common_ids.Label.free])
    assert sris
    return

def test_query_cat_0(user,session):
    """ Test simple text query """
    sris = recommender.search_dapps(session,query='tax',categories=[common_ids.CatalogCategory.health],desired_labels=None)
    assert not sris,'tax is not in health'
    return

def test_cat_1(user,session):
    """ Test simple catalog query """
    sris = recommender.search_dapps(session,query=None,categories=[common_ids.CatalogCategory.living],desired_labels=None)
    assert sris

def test_cat_label(user,session):
    """ Test simple catalog query """
    sris = recommender.search_dapps(session,query=None,categories=[common_ids.CatalogCategory.living],desired_labels=[common_ids.Label.anonymous])
    assert sris

pillars.DAppManager.DAppsById

if __name__ == '__main__':
    test_all(user,session)