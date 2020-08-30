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
    assert next(core.NodeRegistry.get_next_node(ddhkey))[0] is node_c
    assert core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)[0].nschema is schema 
    return

def test_schema_node():
    """ Retrieval of schema and application of get_schema() 
    """
    schema = core.Schema()
    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    node_s = core.Node(schema=schema,owner=user)
    core.NodeRegistry[core.DDHkey(key='/ddh/health')] = node_s
    ddhkey = core.DDHkey(key='/ddh/health/mgf/bmi/weight')
    node_s,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    assert node_s.nschema is schema
    assert node_s.get_schema(ddhkey,split) is schema

def test_basic_access():

    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = core.User(id='2',name='roman',email='roman.stoessel@swisscom.com')
    node_o = core.Node(owner=user)
    node_c = core.Node(consent=core.Consent(grantedTo=[user2]),owner=user)    
    ddhkey = core.DDHkey(key='root')
    ddhkey2 = core.DDHkey(key='root/unknown')

    core.NodeRegistry[ddhkey2] = node_o 
    core.NodeRegistry[ddhkey] = node_c
    access = core.Access(ddhkey=ddhkey,principal=user)
    
    assert access.permitted()[0]

    access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=[core.AccessMode.read_for_write,core.AccessMode.anonymous])
    #access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=core.AccessModeF.read|core.AccessModeF.anonymous)
    assert not access2.permitted()[0]
    return

if __name__ == '__main__':
    test_nodes()