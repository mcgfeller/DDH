
import pydantic
import datetime
import typing
import enum
import json
import pydantic_core
from fastapi.encoders import jsonable_encoder


class Model1(pydantic.BaseModel):

    i: int
    k: str | None = 'default'


class Model2(Model1):
    e: float


class ModelCollection(pydantic.BaseModel):
    m1: Model1
    m2: Model2 | None = Model2(i=1, e=3.1415)


def build():
    mc = ModelCollection(m1=Model1(i=42, k='test'))
    return mc


def test():
    ModelCollection.model_json_schema()


class ModelRef(pydantic.BaseModel):
    class Config:
        json_schema_extra = {
            "$ref": pydantic.AnyUrl("file:///schema.json#/$defs/ModelType")
        }


schema: dict = ModelRef.model_json_schema()
print(json.dumps(pydantic_core.to_jsonable_python(schema, serialize_unknown=True)))
print(json.dumps(jsonable_encoder(schema)))
