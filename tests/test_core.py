from .. import core
from . import test_data

def test_basic_access():

    user = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = core.User(id='2',name='roman',email='roman.stoessel@swisscom.com')

    ddhkey = core.DDHkey(key='unknown',owner=user)
    access = core.Access(ddhkey=ddhkey,principal=user)
    assert access.permitted()

    access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=[core.AccessMode.read_for_write,core.AccessMode.anonymous])
    #access2 = core.Access(ddhkey=ddhkey,principal=user2,mode=core.AccessModeF.read|core.AccessModeF.anonymous)
    assert not access2.permitted()
    return

if __name__ == '__main__':
    test_basic_access()