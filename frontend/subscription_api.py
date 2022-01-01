""" This is the Subscription API, to register and handle user subscriptions and consents.
    The main client is the Client User interface.


"""

import fastapi
import fastapi.security
import typing
import pydantic
import datetime
import enum


from core import pillars,dapp
from core import keys,permissions,schemas,facade,errors,principals
from frontend import sessions

app = fastapi.FastAPI()

from frontend import user_auth # provisional user management

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



@app.post("/subscriptions/dapp/{dappid:principals.DAppId}",response_model=list[dapp.DAppOrFamily])
async def create_subscription(
    dappid : principals.DAppId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ Create a single subscription for a user """
    dapp = pillars.DAppManager.DAppsById.get(dappid)
    if not dapp:
        raise fastapi.HTTPException(status_code=404, detail=f"DApp not found: {dappid}.")
    
