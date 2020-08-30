""" Set up some Test data """
import core
import pillars



def test_dapp():
    ddhkey = core.DDHkey(key="/ddh/shopping/stores/migros/receipts/mgf")
    snode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    schema = snode.get_schema(ddhkey,split)
    return


test_dapp()