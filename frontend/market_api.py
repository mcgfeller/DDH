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
from core import keys, permissions, schemas, facade, errors, principals, common_ids, users
from frontend import sessions
from frontend import user_auth  # provisional user management
from market import recommender
from utils import fastapi_utils

app = fastapi.FastAPI()


@app.get("/health")
async def health():
    return {'status': 'ok'}


@app.get("/users/me/", response_model=users.User)
async def read_users_me(current_user: user_auth.UserInDB = fastapi.Depends(user_auth.get_current_active_user)):
    """ return my user """
    return current_user.as_user()

# get user_auth.login_for_access_token defined in app:


@app.post("/token", response_model=user_auth.Token)
async def login_for_access_token(form_data: fastapi.security.OAuth2PasswordRequestForm = fastapi.Depends()):
    user, dappid, token = await user_auth.login_for_access_token(form_data)
    # Create access record:
    access = permissions.Access(ddhkey=keys.DDHkey('/login'), principal=user,
                                modes={permissions.AccessMode.login}, byDApp=dappid)
    return token


@app.get("/market/dapp", response_model=list[recommender.SearchResultItem])
async def get_dapps(
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    query: str = fastapi.Query(None, min_length=3, max_length=100),
    categories: list[common_ids.CatalogCategory | None] = fastapi.Query(None),
    desired_labels: list[common_ids.Label | None] = fastapi.Query(None),
):
    """ search for DApps or DApp Families """
    all_dapps = await get_all_dapps(session)
    sub_dapps = await get_subscriptions(session)
    sris = await recommender.search_dapps(session, all_dapps, sub_dapps, query, categories, desired_labels)
    return sris


@app.get("/market/dapp/{dappid:principals.DAppId}", response_model=dapp_attrs.DAppOrFamily)
async def get_dapp(
    dappid: principals.DAppId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
):
    """ get a single DApp or DApp Family by its ID """
    dapp = await get_all_dapps(session, dappid=dappid)
    if dapp:
        return dapp
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"DApp not found: {dappid}.")


async def get_all_dapps(session: sessions.Session, dappid: principals.DAppId | None = None) -> typing.Sequence[dapp_attrs.DApp]:
    url = '/dapp'+(f'/{dappid}' if dappid else '')
    d = await fastapi_utils.submit1_asynch(session, 'http://localhost:8001', url, params={'attrs': 'True'})
    das = [dapp_attrs.DApp(**da) for da in d]
    return das


async def get_subscriptions(session: sessions.Session) -> frozenset[str]:
    user = session.user.id
    d = await fastapi_utils.submit1_asynch(session, 'http://localhost:8003', f'/users/{user}/subscriptions/dapp/')
    return frozenset(d)


if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 8002
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
