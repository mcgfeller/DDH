""" Set up some Test data """
import core
import pillars



def test_dapp():
    """ test retrieval of key of test MigrosDApp, and core.get_schema() """
    ddhkey = core.DDHkey(key="/ddh/shopping/stores/migros/clients/receipts")
    jschema = core.get_schema(ddhkey)
    assert isinstance(jschema,dict)
    assert jschema['title'] == 'Receipt' # type: ignore
    return


def test_complete_schema():
    ddhkey = core.DDHkey(key="/ddh/shopping")
    s = core.get_schema(ddhkey)
    assert s
    return s