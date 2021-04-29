from core import keys,nodes,permissions,schemas,facade,keydirectory
from . import  test_data
import pytest

def test_paths():
    ddhkey1 = keys.DDHkey(key='norooted')
    ddhkey2 = keys.DDHkey(key='norooted/subkey')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = keys.DDHkey(key='/rooted')
    ddhkey4 = keys.DDHkey(key='/rooted/subkey')
    assert ddhkey3 == ddhkey4.up()
    assert ddhkey3.up().up() is None
    ddhkey = keys.DDHkey(key=())

    return

class DummyElement(schemas.SchemaElement): ...

def test_nodes():
    schema = schemas.PySchema(schema_element=DummyElement)
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = permissions.User(id='2',name='roman',email='roman.stoessel@swisscom.com')
    node_s = nodes.Node(schema=schema,owner=user)
    node_c = nodes.Node(consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])]),owner=user)    
    keydirectory.NodeRegistry[keys.DDHkey(key='/p/health')] = node_s
    keydirectory.NodeRegistry[keys.DDHkey(key='/p/health/mgf')] = node_c    
    ddhkey = keys.DDHkey(key='/p/health/mgf/bmi/weight')
    assert next(keydirectory.NodeRegistry.get_next_node(ddhkey,nodes.NodeType.consents))[0] is node_c
    assert keydirectory.NodeRegistry.get_node(ddhkey,nodes.NodeType.nschema)[0].nschema is schema 
    return

def test_schema_node():
    """ Retrieval of schema and application of get_sub_schema() 
    """
    schema = schemas.PySchema(schema_element=DummyElement)
    user = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    node_s = nodes.Node(schema=schema,owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='/p/health')] = node_s
    ddhkey = keys.DDHkey(key='/p/health/mgf/bmi/weight') # does not exist
    node_s,split = keydirectory.NodeRegistry.get_node(ddhkey,nodes.NodeType.nschema)
    assert node_s.nschema is schema
    assert node_s.get_sub_schema(ddhkey,split) is None
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes=[permissions.AccessMode.schema_read])
    assert facade.get_schema(access) is None # this should be same in one go.


if __name__ == '__main__':
    test_nodes()