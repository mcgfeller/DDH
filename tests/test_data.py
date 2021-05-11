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

def test_dapp_schema(user):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    jschema = facade.get_schema(access)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema(user):
    ddhkey = keys.DDHkey(key="/org/living")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    s = facade.get_schema(access)
    assert s
    return s

def test_dapp_read_data(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.perform_access() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.perform_access(access,session)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['mgf'],list)
    assert len(data['mgf'])>10
    assert all(a in data['mgf'][5] for a in ('Datum_Zeit','Menge','Filiale')) # these keys must be present
    
    return

def test_dapp_read_data_no_owner(user,session):
    """ test retrieval of key of test MigrosDApp, and facade.perform_access() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.perform_access(access,session)
    return

def test_dapp_read_data_unknown(user,session):
    """ test retrieval of key of test MigrosDApp, with a user that does not exist """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf,unknown/receipts")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.perform_access(access,session)
    return

def test_dapp_read_data_nopermit(user,session):
    """ test retrieval of key of test MigrosDApp, with a user that has no permission """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf,another/receipts")
    assert user_auth.UserInDB.load('another')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.AccessError):
        data = facade.perform_access(access,session)
    return

def test_std_read_data(user,session):
    """ test retrieval of key of test MigrosDApp with transformation to standard, and facade.perform_access() """
    ddhkey = keys.DDHkey(key="/p/living/shopping/receipts/mgf")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.perform_access(access,session)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['items'],list)
    assert len(data['items'])>10
    assert all(a in data['items'][5] for a in ('article','quantity','buyer')) # these keys must be present
    
    return

if __name__ == '__main__':
    test_std_read_data()