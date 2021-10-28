""" Set up some Test data """
from core import keys,permissions,facade,errors
from core import pillars
from frontend import user_auth,sessions
import pytest
import json


@pytest.fixture(scope="module")
def user():
    return user_auth.UserInDB.load('mgf')

@pytest.fixture(scope="module")
def user2():
    return user_auth.UserInDB.load('another')

@pytest.fixture(scope="module")
def user3():
    return user_auth.UserInDB.load('another3')

@pytest.fixture(scope="module")
def session(user):
    return sessions.Session(token_str='test_session',user=user)




def test_write_data(user,session):
    """ test write through facade.ddh_put() """
    ddhkey = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    facade.ddh_put(access,session,data)

    
    return

def test_write_data_other_owner(user,session):
    """ test write through facade.ddh_put() using another owenr"""
    ddhkey = keys.DDHkey(key="/another/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    with pytest.raises(errors.AccessError):
        facade.ddh_put(access,session,data)
    return

def test_set_consent_top(user,user2,session):
    """ test set consent at top """
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    facade.ddh_put(access,session,consents.json())

def test_set_consent_deep(user,user2,user3,session):
    """ test set consent deeper in tree """
    # first set at top:
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    facade.ddh_put(access,session,consents.json())

    # now withdraw access for user2 to a specific document, but give user3 access:
    ddhkey2 = keys.DDHkey(key="/mgf/org/private/documents:consents")
    access2 = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents2 =permissions.Consents(consents=[permissions.Consent(grantedTo=[user3])])
    facade.ddh_put(access2,session,consents2.json())    


def test_write_data_with_consent(user,user2,session):
    """ test write through facade.ddh_put() with three objects:
        - mgf/.../doc1
        - another/.../doc2 with read grant to user
        - another/.../doc3
    """
    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey1,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    facade.ddh_put(access,session,data)

    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc2")
    access = permissions.Access(ddhkey=ddhkey2,principal=user2,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    facade.ddh_put(access,session,data)
    # grant read access to user1
    consents = permissions.Consent.single(grantedTo=[user],withModes={permissions.AccessMode.read})
    ddhkey2f = ddhkey2 ; ddhkey2f.fork = keys.ForkType.consents
    access = permissions.Access(ddhkey=ddhkey2f,principal=user2,modes={permissions.AccessMode.consent_write})
    facade.ddh_put(access,session,consents.json())

    ddhkey3 = keys.DDHkey(key="/another/org/private/documents/doc3")
    access = permissions.Access(ddhkey=ddhkey3,principal=user2,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much more'})
    facade.ddh_put(access,session,data)
  
    return

def test_read_and_write_data(user,user2,session):
    # first, set up some data:
    test_write_data_with_consent(user,user2,session)

    session.new_transaction(for_user=user)

    ddhkey1 = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey1,principal=user,modes={permissions.AccessMode.read})
    facade.ddh_get(access,session)

    # we have grant to read:
    ddhkey2 = keys.DDHkey(key="/another/org/private/documents/doc2")
    access = permissions.Access(ddhkey=ddhkey2,principal=user2,modes={permissions.AccessMode.read})
    facade.ddh_get(access,session)

    # we can write both docs to user
    ddhkeyW1 = keys.DDHkey(key="/mgf/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW1,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'no need to be related'})
    facade.ddh_put(access,session,data)    

    # but not to user2 as user:
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW2,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'no need to be related'})
    with pytest.raises(errors.AccessError):
        facade.ddh_put(access,session,data)    

    # and not as user2:
    access = permissions.Access(ddhkey=ddhkeyW2,principal=user2,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'no need to be related'})
    facade.ddh_put(access,session,data)  

    # even with a new transaction
    session.new_transaction(for_user=user2)
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW2,principal=user2,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'no need to be related'})
    facade.ddh_put(access,session,data)    

    # but with a reinit
    # session.reinit()
    session.new_transaction(for_user=user2)
    ddhkeyW2 = keys.DDHkey(key="/another/org/private/documents/docnew")
    access = permissions.Access(ddhkey=ddhkeyW2,principal=user2,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'no need to be related'})
    facade.ddh_put(access,session,data)    
    return