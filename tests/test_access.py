from .. import core
from . import test_data
import pytest


class DummyElement(core.SchemaElement): ...

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
    test_basic_access()