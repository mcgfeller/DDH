""" This is the API Data Apps may use. It provides the Walled Garden and defines all communication against the DDH trees.
    This is the complete external communication, except for:
    - Data App connecting to its its external service 
    - Data App serving a User Interface
"""

import fastapi
import fastapi.security
import typing
import pydantic
import datetime
import enum


from core import pillars
from core import keys,permissions,schemas,dapp,facade,errors,transactions
from frontend import sessions

app = fastapi.FastAPI()

from frontend import user_auth # provisional user management

@app.get("/users/me/", response_model=permissions.User)
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

@app.get("/ddh{docpath:path}:schema")
async def get_schema(
    docpath: str = fastapi.Path(..., title="The ddh key of the schema to get"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    dapp : typing.Optional[permissions.DAppId] = None,
    schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json, # type: ignore # dynamic
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    access = permissions.Access(ddhkey=keys.DDHkey(docpath),principal=session.user, modes = {permissions.AccessMode.schema_read},byDApp=dapp)
    fschema = facade.get_schema(access,session,schemaformat)
    if not fschema: 
        raise fastapi.HTTPException(status_code=404, detail=f"No schema found at {access.ddhkey}.")
    else:
        return {"ddhkey": access.ddhkey, 'schema': fschema}

@app.get("/ddh{docpath:path}")
async def get_data(
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    modes: set[permissions.AccessMode] = {permissions.AccessMode.read},
    q: str = fastapi.Query(None, alias="item-query"),
    ):

    access = permissions.Access(op = permissions.Operation.get, ddhkey = keys.DDHkey(docpath),principal=session.user, modes = modes, byDApp=session.dappid)
    try:
        d = facade.ddh_get(access,session,q)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"ddhkey": access.ddhkey, "res": d}

@app.put("/ddh{docpath:path}")
async def put_data(
    data: pydantic.Json,
    docpath: str = fastapi.Path(..., title="The ddh key of the data to put"),
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    modes: set[permissions.AccessMode] = {permissions.AccessMode.write},
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    access = permissions.Access(op = permissions.Operation.put, ddhkey = keys.DDHkey(docpath),principal=session.user, modes = modes, byDApp=session.dappid)
    try:
        d = facade.ddh_put(access,session,data,q)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"ddhkey": access.ddhkey, "res": d}

@app.post("/transaction")
async def create_transaction(
    for_user : permissions.Principal,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),
    dapp : typing.Optional[permissions.DAppId] = None,

    ):    
    try:
        trx = session.new_transaction(for_user)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"transaction": trx.trxid}  

