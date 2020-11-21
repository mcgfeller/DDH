import fastapi
import typing
import pydantic
import datetime
import enum


from core import pillars
from core import core

app = fastapi.FastAPI()

import user_auth # provisional user management

@app.get("/users/me/", response_model=core.User)
async def read_users_me(current_user: core.User = fastapi.Depends(user_auth.get_current_user)):
    """ return my user """
    return current_user

# get user_auth.login_for_access_token defined in app: 
app.post("/token", response_model=user_auth.Token)(user_auth.login_for_access_token)


@app.get("/data/{docpath:path}")
async def get_data(
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    user: core.User = fastapi.Depends(user_auth.get_current_active_user),
    dapp : typing.Optional[core.DAppId] = None,
    modes: set[core.AccessMode] = {core.AccessMode.read},
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    if core.AccessMode.read not in modes: # get_data requires read-access
        modes.add(core.AccessMode.read)
    access = core.Access(ddhkey = core.DDHkey(docpath),principal=user, modes = modes,byDApp=dapp)
    d = core.get_data(access,q)
    return {"ddhkey": access.ddhkey, "res": d}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: str = fastapi.Path(..., title="The ddh key of the schema to get"),
    user: core.User = fastapi.Depends(user_auth.get_current_active_user),
    dapp : typing.Optional[core.DAppId] = None,
    schemaformat: core.SchemaFormat = core.SchemaFormat.json, # type: ignore # dynamic
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    
    access = core.Access(ddhkey=core.DDHkey(docpath),principal=user, modes = {core.AccessMode.schema_read},byDApp=dapp)
    ok,consent,text = access.permitted()
    if not ok:
        raise fastapi.HTTPException(status_code=403, detail=f"No access to schema at {access.ddhkey}: {text}")
    fschema = core.get_schema(access,schemaformat)
    if not fschema:
        raise fastapi.HTTPException(status_code=404, detail=f"No schema found at {access.ddhkey}.")
    else:
        return {"ddhkey": access.ddhkey, 'schema': fschema}
   