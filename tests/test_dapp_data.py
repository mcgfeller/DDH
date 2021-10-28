""" Set up some Test data """
from core import keys,permissions,facade,errors
from core import pillars
from frontend import user_auth,sessions
import pytest

@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')

@pytest.fixture(scope="module")
def session(user):
    return sessions.Session(token_str='test_session',user=user)

def test_dapp_schema(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    ddhkey = keys.DDHkey(key="//org/migros.ch/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    jschema = facade.get_schema(access,session)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema_org(user,session):
    ddhkey = keys.DDHkey(key="//org")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    s = facade.get_schema(access,session)
    assert s
    return s

def test_complete_schema_p(user,session):
    ddhkey = keys.DDHkey(key="//p/living/shopping")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    s = facade.get_schema(access,session)
    assert s
    return s


def test_dapp_read_data(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.ddh_get() """
    ddhkey = keys.DDHkey(key="/mgf/org/migros.ch/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.ddh_get(access,session)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['mgf'],list)
    assert len(data['mgf'])>10
    assert all(a in data['mgf'][5] for a in ('Datum_Zeit','Menge','Filiale')) # these keys must be present
    
    return

def test_dapp_read_data_no_owner(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.ddh_get() """
    ddhkey = keys.DDHkey(key="//org/migros.ch/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.ddh_get(access,session)
    return

def test_dapp_read_data_unknown(user,session):
    """ test retrieval of key of test MigrosDApp, with a user that does not exist """
    ddhkey = keys.DDHkey(key="/mgf,unknown/org/migros.ch/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.ddh_get(access,session)
    return

def test_dapp_read_data_nopermit(user,session):
    """ test retrieval of key of test MigrosDApp, with a user that has no permission """
    ddhkey = keys.DDHkey(key="/another/org/migros.ch/receipts")
    assert user_auth.UserInDB.load('another')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.AccessError):
        data = facade.ddh_get(access,session)
    return

def test_std_read_data(user,session):
    """ test retrieval of key of test MigrosDApp with transformation to standard, and facade.ddh_get() """
    ddhkey = keys.DDHkey(key="/mgf/p/living/shopping/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.ddh_get(access,session)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['items'],list)
    assert len(data['items'])>10
    assert all(a in data['items'][5] for a in ('article','quantity','buyer')) # these keys must be present
    
    return

if __name__ == '__main__':
    test_std_read_data(user,session)