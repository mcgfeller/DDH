""" Set up some Test data """
import core
import pillars



def test_dapp():
    """ test retrieval of key of test MigrosDApp, and get_sub_schema() """
    ddhkey = core.DDHkey(key="/ddh/shopping/stores/migros/clients/receipts")
    snode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    schema = snode.get_sub_schema(ddhkey,split)
    assert isinstance(schema,core.JsonSchema)
    assert schema.json_schema['title'] == 'Receipt'
    return


