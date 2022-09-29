""" This is the Subscription API, to register and handle user subscriptions and consents.
    The main client is the Client User interface.


"""

import typing

import fastapi
import fastapi.security

from core import (dapp_attrs, errors, keys, permissions, principals,
                  common_ids)
from user import subscriptions
from frontend import sessions
import httpx

app = fastapi.FastAPI()

from frontend import user_auth  # provisional user management

SUBSCRIPTIONS : dict[common_ids.PrincipalId,dict[principals.DAppId,typing.Any]] = {}

@app.get("/health")
async def health():
    return {'status':'ok'}

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



@app.post("/users/{user}/subscriptions/dapp/{dappid}",response_model=list[str])
async def create_subscription(
    user: common_ids.PrincipalId,
    dappid : str,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ Create a single subscription for a user """
    if not user == session.user.id:
        raise errors.AccessError('authorized user is not ressource owner')
    valid_dappids = await get_dappids(session)
    das =subscriptions.add_subscription(user,typing.cast(principals.DAppId,dappid),valid_dappids)

    return das
    
@app.get("/users/{user}/subscriptions/dapp/",response_model=list[str])
async def list_subscription(
    user: common_ids.PrincipalId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ List subscriptions for a user """
    if not user == session.user.id:
        raise errors.AccessError('authorized user is not ressource owner')
    valid_dappids = await get_dappids(session)    
    das = subscriptions.list_subscriptions(user,valid_dappids)    
    return das

async def get_dappids(session: sessions.Session):
    client = httpx.AsyncClient(base_url='http://localhost:8001')
    j = await client.get('/dapp') 
    j.raise_for_status()
    d = j.json()
    return set(d)

if __name__ == "__main__": # Debugging
    import uvicorn
    import os
    port = 8003
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
