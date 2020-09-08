import fastapi
import typing
import pydantic
import datetime
import core
import pillars

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
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    ddhkey = core.DDHkey(docpath).ensure_rooted()
    enode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.execute)
    enode = typing.cast(core.ExecutableNode,enode)
    d = enode.execute(user, q)
    return {"ddhkey": ddhkey, "res": d}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: str = fastapi.Path(..., title="The ddh key of the schema to get"),
    user: core.User = fastapi.Depends(user_auth.get_current_active_user),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    ddhkey = core.DDHkey(docpath).ensure_rooted()
    snode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    if snode:
        schema = snode.get_schema(ddhkey,split)
        return {"ddhkey": ddhkey, 'schema': schema}
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"No schema not found at {ddhkey}.")