from .. import model
from . import test_data

def test_basic_access():

    user = model.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = model.User(id='2',name='roman',email='roman.stoessel@swisscom.com')

    ddhkey = model.DDHkey(key='unknown',owner=user)
    access = model.Access(ddhkey=ddhkey,principal=user)
    assert access.permitted()

    access2 = model.Access(ddhkey=ddhkey,principal=user2,mode=[model.AccessMode.read_for_write,model.AccessMode.anonymous])
    #access2 = model.Access(ddhkey=ddhkey,principal=user2,mode=model.AccessModeF.read|model.AccessModeF.anonymous)
    assert not access2.permitted()
    return

if __name__ == '__main__':
    test_basic_access()