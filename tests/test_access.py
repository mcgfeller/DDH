from core import keys,nodes,permissions,schemas
from . import test_data
import pytest


class DummyElement(schemas.SchemaElement): ...

def test_basic_access():
    """ Test permissions of two nodes, with basic grant to another user """

    user1 = permissions.User(id='1',name='martin',email='martin.gfeller@swisscom.com')
    user2 = permissions.User(id='2',name='roman',email='roman.stoessel@swisscom.com')
    user3 = permissions.User(id='3',name='patrick',email='patrick.keller@swisscom.com')

    node_c = nodes.Node(consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])]),owner=user1)    
    ddhkey1 = keys.DDHkey(key='/root')
    nodes.NodeRegistry[ddhkey1] = node_c

    node_o = nodes.Node(owner=user1)
    ddhkey2 = keys.DDHkey(key='/root/unknown')
    nodes.NodeRegistry[ddhkey2] = node_o 

    for ddhkey in (ddhkey1,ddhkey2):

        access = permissions.Access(ddhkey=ddhkey,principal=user1) # node owned by user1
        assert access.permitted()[0]

        access = permissions.Access(ddhkey=ddhkey,principal=user2) # read granted to user2
        assert access.permitted()[0] 

        access = permissions.Access(ddhkey=ddhkey,principal=user2,modes=[permissions.AccessMode.write])
        assert not access.permitted()[0] # write not granted to user2

        access = permissions.Access(ddhkey=ddhkey,principal=user3) # read granted to user2
        assert not access.permitted()[0] 

    return


def test_access_modes():

    users = [permissions.User(id=str(id),name='user'+str(id),email='user'+str(id)+'@dummy.com') for id in range(7)]
    AM = permissions.AccessMode
    node_c = nodes.Node(consents=permissions.Consents(consents=[
        permissions.Consent(grantedTo=[users[1]]),
        permissions.Consent(grantedTo=[users[2]],withModes={AM.read}),
        permissions.Consent(grantedTo=[users[3]],withModes={AM.write}),    
        permissions.Consent(grantedTo=[users[4]],withModes={AM.read, AM.write, AM.protected}),
        permissions.Consent(grantedTo=[users[5]],withModes={AM.read, AM.write, permissions.AccessMode.anonymous}),  
        permissions.Consent(grantedTo=[users[6]],withModes={AM.read, AM.write, AM.protected,permissions.AccessMode.pseudonym}),           
        ]),owner=users[0])    
    ddhkey = keys.DDHkey(key='/root')
    nodes.NodeRegistry[ddhkey] = node_c


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
        access = permissions.Access(ddhkey=ddhkey,principal=users[user],modes=modes)
        rok,consent,explanation = access.permitted()
        diagnose = f'Test {i} result {rok} expected {ok} because {comment or "it is obvious"}: {explanation}, for {user=}, {modes=}, {consent=}'
        if  rok != ok: 
            print(diagnose)
        assert rok == ok,diagnose
    return

if __name__ == '__main__':
    test_basic_access()