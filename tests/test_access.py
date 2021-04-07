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

@pytest.fixture
def users():
    users = [permissions.User(id=str(id),name='user'+str(id),email='user'+str(id)+'@dummy.com') for id in range(7)]
    return users

@pytest.fixture
def ddhkey_setup(users):
    """ return ddhkey, with Node set up """
    AM = permissions.AccessMode
    node_c_s = nodes.Node(owner=users[0],
        consents=permissions.Consents(consents=[
            permissions.Consent(grantedTo=[users[1]]),
            permissions.Consent(grantedTo=[users[2]],withModes={AM.read}),
            permissions.Consent(grantedTo=[users[3]],withModes={AM.write}),    
            permissions.Consent(grantedTo=[users[4]],withModes={AM.read, AM.write, AM.protected}),
            permissions.Consent(grantedTo=[users[5]],withModes={AM.read, AM.write, permissions.AccessMode.anonymous}),  
            permissions.Consent(grantedTo=[users[6]],withModes={AM.read, AM.write, AM.protected,permissions.AccessMode.pseudonym}),           
        ]))    
    ddhkey_s = keys.DDHkey(key='/root/single_owner')
    nodes.NodeRegistry[ddhkey_s] = node_c_s

    node_c_m = nodes.MultiOwnerNode(all_owners=users[0:2],
        consents=permissions.MultiOwnerConsents(consents_by_owner = {
        users[0] : permissions.Consents(consents=[
            permissions.Consent(grantedTo=[users[1]]),
            permissions.Consent(grantedTo=[users[2]],withModes={AM.read}),
            permissions.Consent(grantedTo=[users[3]],withModes={AM.write}),    
            permissions.Consent(grantedTo=[users[4]],withModes={AM.read, AM.write, AM.protected}),
            permissions.Consent(grantedTo=[users[5]],withModes={AM.read, AM.write, permissions.AccessMode.anonymous}),  
            permissions.Consent(grantedTo=[users[6]],withModes={AM.read, AM.write, AM.protected,permissions.AccessMode.pseudonym}),           
            ]),
        users[1] : permissions.Consents(consents=[
            permissions.Consent(grantedTo=[users[2]]),
            ]), 
        }))   
    ddhkey_m = keys.DDHkey(key='/root/multi_owner')
    nodes.NodeRegistry[ddhkey_m] = node_c_m

    return [ddhkey_s,ddhkey_m]


# Setup list of tests: 
AM = permissions.AccessMode
test_params = [ 
      (True,0,0,{AM.read,AM.write},'accessor is owner'),
      (True,0,0,{AM.read,AM.anonymous},'anonymous reader is owner'),
      (True,0,1,{AM.read},'reading user has full grant'),
      (False,0,1,{AM.write},'writing user has full grant'),
      (True,0,1,{AM.read,AM.anonymous},'read includes anonymous read'), 
      (True,0,1,{AM.read,AM.pseudonym},'read includes pseudonymous read'), 
      (False,0,1,{AM.write,AM.anonymous},'no write permission'), 
      (True,0,2,{AM.read},'reading user has read grant'),
      (False,0,2,{AM.write},'writing user has only read grant'),      
      (False,0,3,{AM.read},'write doesn\'t imply read'), 
      (True,0,3,{AM.write},'writing user has only write gran'),   
      (True,0,4,{AM.read},'protected not required for read'), 
      (True,0,4,{AM.read,AM.protected},'protected is optional'), 
      (False,0,4,{AM.write},'protected is required for write'), 
      (True,0,4,{AM.write,AM.protected},'protected is required for write'), 
      (False,0,5,{AM.read},'must specify anonymous'), 
      (True,0,5,{AM.read,AM.anonymous},'anonymous read granted'),   
      (True,0,6,{AM.read,AM.pseudonym},'pseudonym sufficient in read'), 
      (False,0,6,{AM.read,AM.protected},'must specify pseudonym'), 
      (False,0,6,{AM.write,AM.pseudonym},'protected is required for write'),       
      (True,0,6,{AM.write,AM.protected,AM.pseudonym},'protected is required for write'),       

      (False,1,0,{AM.read,AM.write},'accessor is one owner, but two owners'),
      (True,1,2,{AM.read},'both owners grant reading to user'),
      (False,1,3,{AM.write},'only one user grants writing to user'),
    ]


@pytest.mark.parametrize('ok,obj,user,modes,comment',
    test_params,ids=[f"Obj {d[1]}: {d[4].strip().replace(' ','-')}" if d[4] else None for d in test_params]) # use comment as test id
def test_access_modes(ddhkey_setup,users,ok,obj,user,modes,comment):

    access = permissions.Access(ddhkey=ddhkey_setup[obj],principal=users[user],modes=modes)
    rok,consent,explanation = access.permitted()
    diagnose = f'Test result {rok} expected {ok} because {comment or "it is obvious"}: {explanation}, for {user=}, {modes=}, {consent=}'
    if  rok != ok: 
        print(diagnose)
    assert rok == ok,diagnose
    return

if __name__ == '__main__':
    test_basic_access()