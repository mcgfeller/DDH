""" This is the Market API to retrieve and engage with DApps.
    The main client is the Client User interface and the DApp author interface.
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
from frontend import user_auth # provisional user management
from market import recommender

app = fastapi.FastAPI()



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

@app.get("/market/dapp",response_model=list[dapp.DAppOrFamily])
async def get_dapps(
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    query: str = fastapi.Query(None, min_length=3, max_length=100),
    ):
    """ search for DApps or DApp Families """
    dapps = recommender.search_dapps(session,query)
    return dapps


@app.get("/market/dapp/{dappid:principals.DAppId}",response_model=dapp.DAppOrFamily)
async def get_dapp(
    dappid : principals.DAppId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ get a single DApp or DApp Family by its ID """
    dapp = pillars.DAppManager.DAppsById.get(dappid)
    if dapp:
        return dapp
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"DApp not found: {dappid}.")
