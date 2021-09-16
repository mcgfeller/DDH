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
    """ test write through facade.put_access() """
    ddhkey = keys.DDHkey(key="/mgf/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    facade.put_access(access,session,data)

    
    return

def test_write_data_other_owner(user,session):
    """ test write through facade.put_access() using another owenr"""
    ddhkey = keys.DDHkey(key="/another/org/private/documents/doc1")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    data = json.dumps({'document':'not much'})
    with pytest.raises(errors.AccessError):
        facade.put_access(access,session,data)
    return

def test_set_consent_top(user,user2,session):
    """ test set consent at top """
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    facade.put_access(access,session,consents.json())

def test_set_consent_deep(user,user2,user3,session):
    """ test set consent deeper in tree """
    # first set at top:
    ddhkey = keys.DDHkey(key="/mgf:consents")
    access = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents=permissions.Consents(consents=[permissions.Consent(grantedTo=[user2])])
    facade.put_access(access,session,consents.json())

    # now withdraw access for user2 to a specific document, but give user3 access:
    ddhkey2 = keys.DDHkey(key="/mgf/org/private/documents:consents")
    access2 = permissions.Access(ddhkey=ddhkey,principal=user,modes={permissions.AccessMode.write})
    consents2 =permissions.Consents(consents=[permissions.Consent(grantedTo=[user3])])
    facade.put_access(access2,session,consents2.json())    