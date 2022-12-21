""" This is the API Data Apps may use. It provides the Walled Garden and defines all communication against the DDH trees.
    This is the complete external communication, except for:
    - Data App connecting to its its external service 
    - Data App serving a User Interface
"""

from ensurepip import version
import fastapi
import fastapi.security
import typing
import pydantic
import datetime
import enum


from core import pillars, schema_network
from core import keys, permissions, schemas, facade, errors, principals, versions, dapp_proxy, dapp_attrs, pillars, users
from frontend import sessions

app = fastapi.FastAPI()

from frontend import user_auth  # provisional user management


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


@app.get("/ddh{docpath:path}")
async def get_data(
    response: fastapi.Response,
    request: fastapi.Request,
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    accept: list[str] | None = fastapi.Header(default=None),
    modes: set[permissions.AccessMode] = {permissions.AccessMode.read},
    q: str = fastapi.Query(None, alias="item-query"),
):

    access = permissions.Access(op=permissions.Operation.get, ddhkey=keys.DDHkey(
        docpath), principal=session.user, modes=modes, byDApp=session.dappid)
    try:
        d = await facade.ddh_get(access, session, q, accept)
    except errors.DDHerror as e:
        raise e.to_http()

    response.headers['Content-Location'] = f'{request.url.scheme}://{request.url.netloc}/ddh{str(access.ddhkey)}'
    return d


@app.put("/ddh{docpath:path}")
async def put_data(
    data: pydantic.Json,
    docpath: str = fastapi.Path(..., title="The ddh key of the data to put"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    modes: set[permissions.AccessMode] = {permissions.AccessMode.write},
    q: str = fastapi.Query(None, alias="item-query"),
):
    access = permissions.Access(op=permissions.Operation.put, ddhkey=keys.DDHkey(
        docpath), principal=session.user, modes=modes, byDApp=session.dappid)
    try:
        d = facade.ddh_put(access, session, data, q)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"ddhkey": access.ddhkey, "res": d}


@app.post("/transaction")
async def create_transaction(
    for_user: principals.Principal,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    dapp: principals.DAppId | None = None,

):
    try:
        trx = session.new_transaction(for_user)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"transaction": trx.trxid}


@app.post("/reinitialize")
async def reinitialize(
    for_user: principals.Principal,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

):
    try:
        trx = session.new_transaction(for_user)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"transaction": trx.trxid}


@app.post("/connect")
async def connect_dapp(
    running_dapp: dapp_attrs.RunningDApp,
    # session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    request: fastapi.Request,
    bg_tasks: fastapi.BackgroundTasks,
    session=sessions.get_system_session(),
):
    # we have to return a response so caller can finish startup, before we issue a request against the caller.
    # Therefore, add registration as a background task:
    bg_tasks.add_task(dapp_proxy.DAppManager.register, request, session, running_dapp)
    return


@app.get("/dapp")
async def list_dapps(
    session:  sessions.Session = fastapi.Depends(user_auth.get_current_session),
    attrs: bool = False
) -> list[principals.DAppId] | list[dapp_attrs.DAppOrFamily]:
    """ return a list of DApps """
    if attrs:
        return [dp.attrs for dp in dapp_proxy.DAppManager.DAppsById.values()]
    else:
        return list(dapp_proxy.DAppManager.DAppsById.keys())


@app.get("/dapp/{dappids}")
async def get_dapp(
    dappids: str,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
) -> list[dapp_attrs.DApp]:
    """ return attributes of DApps given one or more DAppIds, separated by '+' """
    dis = dappids.split('+')
    dapps = [dapp.attrs for dappid in dis if (
        dapp := dapp_proxy.DAppManager.DAppsById.get(typing.cast(principals.DAppId, dappid)))]
    return dapps


@app.get("/graph/from/{from_dapps}")
async def dapps_from(
    from_dapps: str,
    details: bool = fastapi.Query(False),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
) -> typing.Iterable[principals.DAppId]:
    schema_network = pillars.Pillars['SchemaNetwork']
    s = []
    for dappid in from_dapps.split('+'):
        dapp = dapp_proxy.DAppManager.DAppsById.get(typing.cast(principals.DAppId, dappid))
        if dapp:
            x1 = schema_network.dapps_from(dapp.attrs, session.user)
            if details:
                s.append(x1)
            else:
                s.append({x.id for x in x1})
    return s


@app.get("/graph/to/{for_dapps}")
async def dapps_required(
    for_dapps: str,
    include_weights: bool = fastapi.Query(False),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
) -> list[tuple[set[principals.DAppId], set[principals.DAppId]]]:
    schema_network = pillars.Pillars['SchemaNetwork']
    s = []
    for dappid in for_dapps.split('+'):
        dapp = dapp_proxy.DAppManager.DAppsById.get(typing.cast(principals.DAppId, dappid))
        if dapp:
            x1, x2 = schema_network.dapps_required(dapp.attrs, session.user)
            if include_weights:
                s.append(({x.id for x in x1}, {x.id for x in x2},
                         {x.id: x.get_weight() for x in x2}))
            else:
                s.append(({x.id for x in x1}, {x.id for x in x2}))
    print(f'dapps_required {for_dapps=}, {s=}')
    return s


if __name__ == "__main__":  # Debugging
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
