""" Standard schemas living """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas,keys,nodes, principals, keydirectory


class Receipt(schemas.SchemaElement):

    buyer: str = pydantic.Field(sensitivity=schemas.Sensitivity.eid)
    article: str
    quantity: float = 1.0
    amount: float = 0.0
    when: datetime.datetime = pydantic.Field(sensitivity=schemas.Sensitivity.sa)
    where: str = pydantic.Field(sensitivity=schemas.Sensitivity.sa)

def create_schema(sel : schemas.SchemaElement, schema_attributes: schemas.SchemaAttributes | None):
    # TODO: This is SchemaElement.replace_by_schema
    s = schemas.PySchema(schema_attributes=schema_attributes or schemas.SchemaAttributes(), schema_element=cls)

def create_node(ddhkey : keys.DDHkey, schema: schemas.PySchema):
    ddhkey = ddhkey.ensure_fork(keys.ForkType.schema)
    node = nodes.SchemaNode(owner=principals.RootPrincipal,consents=schemas.AbstractSchema.get_schema_consents())
    node.add_schema(schema)
    keydirectory.NodeRegistry[ddhkey] = node