from __future__ import annotations
import pydantic
import datetime
import typing
import enum


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
