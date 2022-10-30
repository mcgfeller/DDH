""" Test the recommender, i.e., present a list of recommended DApps based on user input.
"""

from core import common_ids
import pytest



def subscribe(client,dappid):
    """ subscribe user to dappid """
    # TODO: Extract user from client
    r = client.post(f'/users/mgf/subscriptions/dapp/{dappid}')
    r.raise_for_status()
    d = r.json()
    return 

def unsubscribe(client,dappid):
    """ unsubscribe user from dappid """
    r = client.delete(f'/users/mgf/subscriptions/dapp/{dappid}')
    r.raise_for_status()
    d = r.json()
    return 

@pytest.fixture()
def subscribe_migros(user1_sub):
    yield subscribe(user1_sub,'MigrosDApp')
    return unsubscribe(user1_sub,'MigrosDApp')

@pytest.fixture()
def subscribe_scs_emp(user1_sub):
    yield subscribe(user1_sub,'SwisscomEmpDApp')
    return unsubscribe(user1_sub,'SwisscomEmpDApp')


@pytest.fixture()
def subscribe_tax(user1_sub):
    yield subscribe(user1_sub,'TaxCalc')
    return unsubscribe(user1_sub,'TaxCalc')

@pytest.fixture()
def all_dapps(user1):
    j = user1.get('/dapp') 
    j.raise_for_status()
    d = j.json()
    return d


def search(user1_market,query=None,categories=None,desired_labels=None):
    params = {'query':query,'categories':categories,'desired_labels':desired_labels,}
    j = user1_market.get('/market/dapp',params={p:v for p,v in params.items() if v is not None}) 
    j.raise_for_status()
    d = j.json()
    return d


def test_all(user1_market,all_dapps):
    """ Test without criteria """
    sris = search(user1_market,query=None,categories=None,desired_labels=None)
    assert len(sris) == len(all_dapps)
    return

def test_query_1(user1_market):
    """ Test simple text query """
    sris = search(user1_market,query='tax',categories=None,desired_labels=None)
    assert sris
    return

def test_query_label_1(user1_market):
    """ Simple text + fulfilled label """
    sris = search(user1_market,query='tax',categories=None,desired_labels=[common_ids.Label.free.value])
    assert sris
    return

def test_query_label_0(user1_market):
    """  Simple text + non-fulfilled label """
    sris = search(user1_market,query='tax',categories=None,desired_labels=[common_ids.Label.anonymous.value])
    assert sris
    assert sris[0]['merit'] < 0
    assert common_ids.Label.anonymous in sris[0]['ignored_labels']
    return

def test_query_cat_1(user1_market):
    """ Test simple text query """
    sris = search(user1_market,query='tax',categories=[common_ids.CatalogCategory.finance.value],desired_labels=None)
    assert sris
    return

def test_query_cat_label_1(user1_market):
    """ Test simple text query """
    sris = search(user1_market,query='tax',categories=[common_ids.CatalogCategory.finance.value],desired_labels=[common_ids.Label.free.value])
    assert sris
    return

def test_query_cat_0(user1_market):
    """ Test simple text query """
    sris = search(user1_market,query='tax',categories=[common_ids.CatalogCategory.health.value],desired_labels=None)
    assert not sris,'tax is not in health'
    return

def test_cat_1(user1_market):
    """ Test simple catalog query """
    sris = search(user1_market,query=None,categories=[common_ids.CatalogCategory.living.value],desired_labels=None)
    assert sris

def test_cat_label(user1_market):
    """ Test simple catalog query """
    sris = search(user1_market,query=None,categories=[common_ids.CatalogCategory.living.value],desired_labels=[common_ids.Label.anonymous.value])
    assert sris

def test_subscribed(user1_market,subscribe_scs_emp,subscribe_migros):
    """ test with two subscribed apps """
    subscribe_scs_emp,subscribe_migros
    sris = search(user1_market,query=None,categories=None,desired_labels=None)
    assert sris
    assert sris[0].da.id == 'TaxCalc',"as we're subscribed to an employee app, the tax calculator is the best recommendation"
    return


if __name__ == '__main__':
    test_all(user1_market)