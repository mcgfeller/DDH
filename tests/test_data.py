""" Set up some Test data """
from core import core
from core import pillars



def test_dapp():
    """ test retrieval of key of test MigrosDApp, and core.get_schema() """
    ddhkey = core.DDHkey(key="/ddh/shopping/stores/migros/clients/receipts")
    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = core.Access(ddhkey=ddhkey,principal=user,modes={core.AccessMode.schema_read})
    jschema = core.get_schema(access)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema():
    ddhkey = core.DDHkey(key="/ddh/shopping")
    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    access = core.Access(ddhkey=ddhkey,principal=user,modes={core.AccessMode.schema_read})
    s = core.get_schema(access)
    assert s
    return s