""" This is the Market API to retrieve and engage with DApps.
    The main client is the Client User interface and the DApp author interface.
"""

import fastapi
import fastapi.security
import typing
import pydantic
import datetime
import enum
import httpx


from core import dapp_attrs, pillars
from core import keys,permissions,schemas,facade,errors,principals,common_ids
from frontend import sessions
from frontend import user_auth # provisional user management
from market import recommender

app = fastapi.FastAPI()

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

@app.get("/market/dapp",response_model=list[recommender.SearchResultItem])
async def get_dapps(
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    query: str = fastapi.Query(None, min_length=3, max_length=100),
    categories : typing.Optional[typing.Iterable[common_ids.CatalogCategory]] = None, 
    labels : typing.Optional[typing.Iterable[common_ids.Label]] = None, 
    ):
    """ search for DApps or DApp Families """
    all_dapps = await get_dappids(session)
    sub_dapps = await get_subscriptions(session)
    dapps = recommender.search_dapps(session,all_dapps,sub_dapps,query,categories,labels)
    dapps = [d.to_DAppOrFamily() for d in dapps] # convert to result model
    return dapps


@app.get("/market/dapp/{dappid:principals.DAppId}",response_model=dapp_attrs.DAppOrFamily)
async def get_dapp(
    dappid : principals.DAppId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    ):
    """ get a single DApp or DApp Family by its ID """
    dapp = await get_dappids(session,dappid=dappid)
    if dapp:
        return dapp
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"DApp not found: {dappid}.")

async def get_dappids(session: sessions.Session,dappid:typing.Optional[principals.DAppId] = None):
    client = httpx.AsyncClient(base_url='http://localhost:8001')
    url = '/dapp'+f'/{dappid}' if dappid else '' 
    j = await client.get(url,params={'attrs':'True'})
    j.raise_for_status()
    d = j.json()
    return set(d)

async def get_subscriptions(session: sessions.Session):
    user = session.user.id
    client = httpx.AsyncClient(base_url='http://localhost:8003')
    j = await client.get(f'/users/{user}/subscriptions/dapp/') 
    j.raise_for_status()
    d = j.json()
    return set(d)    
