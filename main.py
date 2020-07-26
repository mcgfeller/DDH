import fastapi
import model
import typing
app = fastapi.FastAPI()

class Dhp(str ): 
    def get_key(self) -> typing.Optional[model.DDHkey]:
        user = model.User(id=1,name='martin',email='martin.gfeller@swisscom.com')
        ddhkey = model.DDHkey(key='unknown',owner=user)
        return ddhkey


@app.get("/data/{docpath:path}")
async def get_data(
    docpath: Dhp = fastapi.Path(..., title="The ddh key of the data to get"),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    user = model.User(id=1,name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = model.DDHkey.get_key(docpath)
    return {"ddhkey": ddhkey}

@app.get("/schema/{docpath:path}")
async def get_schema(
    docpath: Dhp = fastapi.Path(..., title="The ddh key of the schema to get"),
    q: str = fastapi.Query(None, alias="item-query"),
    ):
    user = model.User(id=1,name='martin',email='martin.gfeller@swisscom.com')
    ddhkey = model.DDHkey.get_key(docpath)
    return {"ddhkey": ddhkey}