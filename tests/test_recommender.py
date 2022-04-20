""" Set up some Test data """
from core import keys,permissions,facade,errors,principals,common_ids
from core import pillars
from frontend import user_auth,sessions
from market import recommender
from user import subscriptions
import pytest

@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')

@pytest.fixture(scope="module")
def session(user):
    return sessions.Session(token_str='test_session',user=user)

@pytest.fixture()
def subscribe_migros(user):
    yield subscriptions.add_subscription(user,'MigrosDApp')
    return subscriptions.delete_subscription(user,'MigrosDApp')

@pytest.fixture()
def subscribe_scs_emp(user):
    yield subscriptions.add_subscription(user,'SwisscomEmpDApp')
    return subscriptions.delete_subscription(user,'SwisscomEmpDApp')

@pytest.fixture()
def subscribe_tax(user):
    yield subscriptions.add_subscription(user,'TaxCalc')
    return subscriptions.delete_subscription(user,'TaxCalc')


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

def test_subscribed(user,session,subscribe_scs_emp,subscribe_migros):
    """ test with two subscribed apps """
    subscribe_scs_emp,subscribe_migros
    sris = recommender.search_dapps(session,query=None,categories=None,desired_labels=None)
    assert sris
    assert sris[0].da.id == 'TaxCalc'
    return


if __name__ == '__main__':
    test_all(user,session)