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


def install():
    transaction = sessions.get_system_session().get_or_create_transaction()
    return Receipt.insert_as_schema(transaction, keys.DDHkeyGeneric('//p/living/shopping/receipts'))


install()
