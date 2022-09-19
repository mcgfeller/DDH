""" DApp skeleton to run a DApp as a FastAPI microservice """

from __future__ import annotations

import fastapi
import fastapi.security
import httpx
import os


from core import dapp_attrs
from core import keys,permissions,facade,errors,versions,dapp_attrs
from frontend import sessions


router = fastapi.APIRouter()

from frontend import user_auth # provisional user management



CLIENT = httpx.AsyncClient(timeout=5,base_url='http://localhost:8001') # TODO: Configure or determine URL

@router.on_event("startup")
async def startup_event():
    """ Connect ourselves """
    for a in get_apps():
        location = f"http://localhost:{os.environ.get('port')}" # our own port is in the environment
        print(location)
        d = dapp_attrs.RunningDApp(id=a.id,dapp_version=versions.Version(a.version),schema_version=versions.Version('0.0'),location=location)
        await CLIENT.post('connect',data=d.json())
    return



@router.on_event("shutdown")
async def shutdown_event():   
    return


def get_apps() -> tuple[dapp_attrs.DApp]:
    raise NotImplementedError('must be refined by main module')

@router.get("/health")
async def health():
    return {'status':'ok'}

@router.get("/app_info")
async def get_app_info():
    d = {}
    for a in get_apps():
        d[a.id] = a.dict()
    return d

@router.get("/schemas")
async def get_schemas() -> dict:
    a = get_apps()[0]
    s = {str(k): (s.schema_attributes,s.to_output()) for k,s in a.get_schemas().items()}
    return s

@router.post("/execute")
async def execute(req : dapp_attrs.ExecuteRequest):
    return get_apps()[0].execute(req)


@router.get("/provide/ddh{docpath:path}")
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
