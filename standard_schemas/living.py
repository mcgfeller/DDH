""" Standard schemas living """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas, keys, nodes, principals, keydirectory, errors, permissions
from frontend import sessions
from schema_formats import py_schema


class Receipt(py_schema.PySchemaElement):

    buyer: str = pydantic.Field(sensitivity=schemas.Sensitivity.eid)
    article: str
    quantity: float = 1.0
    amount: float = 0.0
    when: datetime.datetime = pydantic.Field(sensitivity=schemas.Sensitivity.sa)
    where: str = pydantic.Field(sensitivity=schemas.Sensitivity.sa)


def install_schema(transaction, ddhkey: keys.DDHkey, sel: typing.Type[py_schema.PySchemaElement], schema_attributes: schemas.SchemaAttributes | None = None):
    # TODO: Almost duplicate with AbstractSchema.insert_schema()
    ddhkey = ddhkey.ensure_fork(keys.ForkType.schema)
    schemaref = sel.replace_by_schema(ddhkey, schema_attributes=schema_attributes)
    pkey = ddhkey.up()
    if not pkey:
        raise errors.NotFound('no parent node')
    access = permissions.Access(ddhkey=pkey, principal=transaction.for_user)
    parent = schemas.SchemaContainer.get_sub_schema(access, transaction)
    assert parent  # must exist because create_intermediate=True

    # now insert our schema into the parent's:
    parent._add_fields({ddhkey[-1]: (schemaref, None)})

    return parent


def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    return install_schema(transaction, keys.DDHkey('//p/living/shopping/receipts'), Receipt)


install()
