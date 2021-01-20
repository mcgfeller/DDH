""" Set up some Test data """
from core import keys,nodes,permissions,facade,errors
from core import pillars
from frontend import user_auth
import pytest



def test_dapp_schema():
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    jschema = facade.get_schema(access)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema():
    ddhkey = keys.DDHkey(key="/org/living")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    s = facade.get_schema(access)
    assert s
    return s

def test_dapp_read_data():
    """ test retrieval of key of test MigrosDApp, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.get_data(access)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['mgf'],list)
    assert len(data['mgf'])>10
    assert all(a in data['mgf'][5] for a in ('Datum_Zeit','Menge','Filiale')) # these keys must be present
    
    return

def test_dapp_read_data_no_owner():
    """ test retrieval of key of test MigrosDApp, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.get_data(access)
    return

def test_dapp_read_data_unknown():
    """ test retrieval of key of test MigrosDApp, with a user that does not exist """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf,unknown/receipts")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.NotFound):
        data = facade.get_data(access)
    return

def test_dapp_read_data_nopermit():
    """ test retrieval of key of test MigrosDApp, with a user that has no permission """
    ddhkey = keys.DDHkey(key="/org/living/stores/migros.ch/clients/mgf,another/receipts")
    user = user_auth.UserInDB.load('mgf')
    assert user_auth.UserInDB.load('another')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.AccessError):
        data = facade.get_data(access)
    return

def test_std_read_data():
    """ test retrieval of key of test MigrosDApp with transformation to standard, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/p/living/shopping/receipts/mgf")
    user = user_auth.UserInDB.load('mgf')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.get_data(access)
    assert isinstance(data,dict)
    assert len(data)>0 
    assert isinstance(data['mgf'],list)
    assert len(data['mgf'])>10
    assert all(a in data['mgf'][5] for a in ('Datum_Zeit','Menge','Filiale')) # these keys must be present
    
    return