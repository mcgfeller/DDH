""" Set up some Test data """
from core import keys,nodes,permissions,facade,errors
from core import pillars
import pytest



def test_dapp_schema():
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    ddhkey = keys.DDHkey(key="/ddh/shopping/stores/migros/clients/receipts")
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    jschema = facade.get_schema(access)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema():
    ddhkey = keys.DDHkey(key="/ddh/shopping")
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.schema_read})
    s = facade.get_schema(access)
    assert s
    return s

def test_dapp_read_data():
    """ test retrieval of key of test MigrosDApp, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/ddh/shopping/stores/migros/clients/1/receipts")
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.get_data(access)
    assert isinstance(data,dict)
    assert len(data)>0 
    return

def test_dapp_read_data_no_owner():
    """ test retrieval of key of test MigrosDApp, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/ddh/shopping/stores/migros/client/receipts")
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    data = facade.get_data(access)
    assert isinstance(data,dict)
    assert len(data)>0 
    return

def test_dapp_read_data_nopermit():
    """ test retrieval of key of test MigrosDApp, and facade.get_data() """
    ddhkey = keys.DDHkey(key="/ddh/shopping/stores/migros/clients/1+99/receipts")
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.read})
    with pytest.raises(errors.AccessError):
        data = facade.get_data(access)
        # assert isinstance(data,dict)
        # assert len(data)>0 
    return