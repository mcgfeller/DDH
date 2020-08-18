import fastapi
import core
import typing
import fastapi.security
app = fastapi.FastAPI()

class Dhp(str ): 
    def get_key(self) -> typing.Optional[core.DDHkey]:
        ddhkey = core.DDHkey(key='unknown')
        return ddhkey

oauth2_scheme = fastapi.security.OAuth2PasswordBearer(tokenUrl="token")

def fake_decode_token(token):
    print('fake_decode_token',token)
    return core.User(
        id=token + "fakedecoded", email="john@example.com", name="John Doe"
    )


async def get_current_user(token: str = fastapi.Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    return user

@app.get("/data/{docpath:path}")
async def get_data(
    docpath: Dhp = fastapi.Path(..., title="The ddh key of the data to get"),
    #user: core.User = fastapi.Depends(get_current_user),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    user = core.User(id='mgf',name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = core.DDHkey(docpath)
    enode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.execute)
    enode = typing.cast(core.ExecutableNode,enode)
    d = enode.execute(user, q)
    return {"ddhkey": ddhkey, "res": d}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: Dhp = fastapi.Path(..., title="The ddh key of the schema to get"),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    user = core.User(id=1,name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = core.DDHkey(docpath)
    snode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    if snode:
        schema = snode.get_schema(ddhkey,split)
        return {"ddhkey": ddhkey, 'schema': schema}
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"No schema not found at {ddhkey}.")