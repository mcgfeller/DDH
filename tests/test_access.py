from core import core
from . import test_data
import pytest


class DummyElement(core.SchemaElement): ...

def test_basic_access():
    """ Test access of two nodes, with basic grant to another user """

    user1 = core.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = core.User(id='2',name='roman',email='roman.stoessel@swisscom.com')
    user3 = core.User(id='3',name='patrick',email='patrick.keller@swisscom.com')

    node_c = core.Node(consents=core.Consents(consents=[core.Consent(grantedTo=[user2])]),owner=user1)    
    ddhkey1 = core.DDHkey(key='/root')
    core.NodeRegistry[ddhkey1] = node_c

    node_o = core.Node(owner=user1)
    ddhkey2 = core.DDHkey(key='/root/unknown')
    core.NodeRegistry[ddhkey2] = node_o 

    for ddhkey in (ddhkey1,ddhkey2):

        access = core.Access(ddhkey=ddhkey,principal=user1) # node owned by user1
        assert access.permitted()[0]

        access = core.Access(ddhkey=ddhkey,principal=user2) # read granted to user2
        assert access.permitted()[0] 

        access = core.Access(ddhkey=ddhkey,principal=user2,modes=[core.AccessMode.write])
        assert not access.permitted()[0] # write not granted to user2

        access = core.Access(ddhkey=ddhkey,principal=user3) # read granted to user2
        assert not access.permitted()[0] 

    return


def test_access_modes():

    users = [core.User(id=str(id),name='user'+str(id),email='user'+str(id)+'@dummy.com') for id in range(7)]
    AM = core.AccessMode
    node_c = core.Node(consents=core.Consents(consents=[
        core.Consent(grantedTo=[users[1]]),
        core.Consent(grantedTo=[users[2]],withModes={AM.read}),
        core.Consent(grantedTo=[users[3]],withModes={AM.write}),    
        core.Consent(grantedTo=[users[4]],withModes={AM.read, AM.write, AM.protected}),
        core.Consent(grantedTo=[users[5]],withModes={AM.read, AM.write, core.AccessMode.anonymous}),  
        core.Consent(grantedTo=[users[6]],withModes={AM.read, AM.write, AM.protected,core.AccessMode.pseudonym}),           
        ]),owner=users[0])    
    ddhkey = core.DDHkey(key='/root')
    core.NodeRegistry[ddhkey] = node_c


    for i,(ok,user,modes,comment) in enumerate((
      (True,0,{AM.read,AM.write},''),
      (True,0,{AM.read,AM.anonymous},''),
      (True,1,{AM.read},''),
      (False,1,{AM.write},''),
      (True,1,{AM.read,AM.anonymous},'read includes anonymous read'), 
      (True,1,{AM.read,AM.pseudonym},'read includes pseudonymous read'), 
      (False,1,{AM.write,AM.anonymous},'no write permission'), 
      (True,2,{AM.read},''),
      (False,2,{AM.write},''),      
      (False,3,{AM.read},'write doesn\'t imply read'), 
      (True,3,{AM.write},''),   
      (True,4,{AM.read},'protected not required for read'), 
      (True,4,{AM.read,AM.protected},'protected is optional'), 
      (False,4,{AM.write},'protected is required for write'), 
      (True,4,{AM.write,AM.protected},'protected is required for write'), 
      (False,5,{AM.read},'must specify anonymous'), 
      (True,5,{AM.read,AM.anonymous},''),   
      (True,6,{AM.read,AM.pseudonym},'pseudonym sufficient in read'), 
      (False,6,{AM.read,AM.protected},'must specify pseudonym'), 
      (False,6,{AM.write,AM.pseudonym},'protected is required for write'),       
      (True,6,{AM.write,AM.protected,AM.pseudonym},'protected is required for write'),       
    )):
        access = core.Access(ddhkey=ddhkey,principal=users[user],modes=modes)
        rok,consent,explanation = access.permitted()
        diagnose = f'Test {i} result {rok} expected {ok} because {comment or "it is obvious"}: {explanation}, for {user=}, {modes=}, {consent=}'
        if  rok != ok: 
            print(diagnose)
        assert rok == ok,diagnose
    return

if __name__ == '__main__':
    test_basic_access()