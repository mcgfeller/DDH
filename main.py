import fastapi
import typing
import fastapi.security

import core
import pillars
app = fastapi.FastAPI()


oauth2_scheme = fastapi.security.OAuth2PasswordBearer(tokenUrl="token")

def fake_decode_token(token):
    # This doesn't provide any security at all
    # Check the next version
    user = get_user(fake_users_db, token)
    return user

def fake_hash_password(password: str):
    return "fakehashed" + password

fake_users_db = {
    "mgf": {
        "id": "mgf",
        "name": "Martin Gfeller",
        "email": "martin.gfeller@swisscom.com",
        "hashed_password": "fakehashedsecret",
    },
    "admin": {
        "id": "admin",
        "name": "DDH admin",
        "email": "martin.gfeller@swisscom.com",
        "hashed_password": "fakehashedsecret",

    },
}

class UserInDB(core.User):
    hashed_password: str

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

async def get_current_user(token: str = fastapi.Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    if not user:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

async def get_current_active_user(current_user: core.User = fastapi.Depends(get_current_user)):
    return current_user

@app.post("/token")
async def login(form_data: fastapi.security.OAuth2PasswordRequestForm = fastapi.Depends()):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise fastapi.HTTPException(status_code=400, detail="Incorrect username or password")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise fastapi.HTTPException(status_code=400, detail="Incorrect username or password")

    return {"access_token": user.id, "token_type": "bearer"}


@app.get("/users/me")
async def read_users_me(current_user: core.User = fastapi.Depends(get_current_active_user)):
    return current_user

@app.get("/data/{docpath:path}")
async def get_data(
    docpath: str = fastapi.Path(..., title="The ddh key of the data to get"),
    #user: core.User = fastapi.Depends(get_current_user),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    user = core.User(id='mgf',name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = core.DDHkey(docpath).ensure_rooted()
    enode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.execute)
    enode = typing.cast(core.ExecutableNode,enode)
    d = enode.execute(user, q)
    return {"ddhkey": ddhkey, "res": d}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: str = fastapi.Path(..., title="The ddh key of the schema to get"),
    q: str = fastapi.Query(None, alias="item-query"),
    token: str = fastapi.Depends(oauth2_scheme)
    ):
    user = core.User(id=1,name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = core.DDHkey(docpath).ensure_rooted()
    snode,split = core.NodeRegistry.get_node(ddhkey,core.NodeType.nschema)
    if snode:
        schema = snode.get_schema(ddhkey,split)
        return {"ddhkey": ddhkey, 'schema': schema, 'token': token}
    else:
        raise fastapi.HTTPException(status_code=404, detail=f"No schema not found at {ddhkey}.")