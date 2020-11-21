""" Set up some Test data """
from core import keys,nodes,permissions,facade
from core import pillars



def test_dapp():
    """ test retrieval of key of test MigrosDApp, and core.get_schema() """
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