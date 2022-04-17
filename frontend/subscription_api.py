""" This is the Subscription API, to register and handle user subscriptions and consents.
    The main client is the Client User interface.


"""

from curses.ascii import SUB
import datetime
import enum
import typing

import fastapi
import fastapi.security
import pydantic
from core import (dapp, errors, facade, keys, permissions, pillars, principals,
                  schemas,common_ids)
from user import subscriptions
from frontend import sessions

app = fastapi.FastAPI()

from frontend import user_auth  # provisional user management

SUBSCRIPTIONS : dict[common_ids.PrincipalId,dict[principals.DAppId,typing.Any]] = {}

@app.get("/users/me/", response_model=principals.User)
async def read_users_me(current_user: user_auth.UserInDB = fastapi.Depends(user_auth.get_current_active_user)):
    """ return my user """
    return current_user.as_user()

# get user_auth.login_for_access_token defined in app:
@app.post("/token", response_model=user_auth.Token)
async def login_for_access_token(form_data: fastapi.security.OAuth2PasswordRequestForm = fastapi.Depends()):
    user,dappid,token =  await user_auth.login_for_access_token(form_data)
    # Create access record:
    access = permissions.Access(ddhkey=keys.DDHkey('/login'),principal=user, modes = {permissions.AccessMode.login},byDApp=dappid)
    return token



@app.post("users/{user}/subscriptions/dapp/{dappid}",response_model=list[dapp.DAppOrFamily])
async def create_subscription(
    user: common_ids.PrincipalId,
    dappid : principals.DAppId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ Create a single subscription for a user """
    if not user == session.user.id:
        raise errors.AccessError('authorized user is not ressource owner')
    subscriptions.add_subscription(user,dappid)

    return [da]
    
@app.get("users/{user}/subscriptions/dapp/",response_model=list[dapp.DAppOrFamily])
async def list_subscription(
    user: common_ids.PrincipalId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ List subscriptions for a user """
    if not user == session.user.id:
        raise errors.AccessError('authorized user is not ressource owner')
    das = subscriptions.list_subscriptions(user)    
    return das
