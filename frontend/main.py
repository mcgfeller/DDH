import fastapi
import typing
import pydantic
import datetime
import enum


from core import pillars
from core import keys,permissions,schemas,dapp,facade,errors

app = fastapi.FastAPI()

from frontend import user_auth # provisional user management

@app.get("/users/me/", response_model=permissions.User)
async def read_users_me(current_user: permissions.User = fastapi.Depends(user_auth.get_current_user)):
    """ return my user """
    return current_user

# get user_auth.login_for_access_token defined in app: 
app.post("/token", response_model=user_auth.Token)(user_auth.login_for_access_token)


@app.get("/data/{docpath:path}")
async def get_data(
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    user: permissions.User = fastapi.Depends(user_auth.get_current_active_user),
    dapp : typing.Optional[permissions.DAppId] = None,
    modes: set[permissions.AccessMode] = {permissions.AccessMode.read},
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    if permissions.AccessMode.read not in modes: # get_data requires read-access
        modes.add(permissions.AccessMode.read)
    access = permissions.Access(ddhkey = keys.DDHkey(docpath),principal=user, modes = modes,byDApp=dapp)
    try:
        d = facade.get_data(access,q)
    except errors.DDHerror as e:
        raise e.to_http()

    return {"ddhkey": access.ddhkey, "res": d}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: str = fastapi.Path(..., title="The ddh key of the schema to get"),
    user: permissions.User = fastapi.Depends(user_auth.get_current_active_user),
    dapp : typing.Optional[permissions.DAppId] = None,
    schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json, # type: ignore # dynamic
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    
    access = permissions.Access(ddhkey=keys.DDHkey(docpath),principal=user, modes = {permissions.AccessMode.schema_read},byDApp=dapp)
    ok,consent,text = access.permitted()
    if not ok: # TODO: Should be errors
        raise fastapi.HTTPException(status_code=403, detail=f"No access to schema at {access.ddhkey}: {text}")
    fschema = facade.get_schema(access,schemaformat)
    if not fschema: # TODO: Should be errors
        raise fastapi.HTTPException(status_code=404, detail=f"No schema found at {access.ddhkey}.")
    else:
        return {"ddhkey": access.ddhkey, 'schema': fschema}
   