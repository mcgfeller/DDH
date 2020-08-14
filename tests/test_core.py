from .. import core
from . import test_data
import pytest

def test_paths():
    ddhkey1 = core.DDHkey(key='norooted')
    ddhkey2 = core.DDHkey(key='norooted/subkey')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = core.DDHkey(key='/rooted')
    ddhkey4 = core.DDHkey(key='/rooted/subkey')
    assert ddhkey3 == ddhkey4.up()
    assert ddhkey3.up().up() is None
    with pytest.raises(ValueError):
        ddhkey = core.DDHkey(key=())

    return

def test_nodes():
    schema = core.Schema()
    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = core.User(id='2',name='roman',email='roman.stoessel@swisscom.com')
    node_s = core.Node(schema=schema,owner=user)
    node_c = core.Node(consent=core.Consent(grantedTo=[user2]),owner=user)    
    core.NodeRegistry[core.DDHkey(key='/ddh/health')] = node_s
    core.NodeRegistry[core.DDHkey(key='/ddh/health/mgf')] = node_c    
    ddhkey = core.DDHkey(key='/ddh/health/mgf/bmi/weight')
    assert next(core.NodeRegistry.get_next_node(ddhkey)) is node_c
    assert core.NodeRegistry.get_node(ddhkey,'nschema').nschema is schema 
    return


def test_basic_access():

    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = core.User(id='2',name='roman',email='roman.stoessel@swisscom.com')

    ddhkey = core.DDHkey(key='unknown')
    access = core.Access(ddhkey=ddhkey,principal=user)
    assert access.permitted()

    access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=[core.AccessMode.read_for_write,core.AccessMode.anonymous])
    #access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=core.AccessModeF.read|core.AccessModeF.anonymous)
    assert not access2.permitted()
    return

if __name__ == '__main__':
    test_nodes()