""" Standard schemas living """


import datetime
import typing

import pydantic

from core import schemas, keys, nodes, principals, keydirectory, errors, permissions
from frontend import sessions
from schema_formats import py_schema


class Receipt(py_schema.PySchemaElement):

    buyer: str = py_schema.SchemaField(sensitivity=schemas.Sensitivity.eid)
    article: str
    quantity: float = 1.0
    amount: float = 0.0
    when: datetime.datetime = py_schema.SchemaField(sensitivity=schemas.Sensitivity.sa)
    where: str = py_schema.SchemaField(sensitivity=schemas.Sensitivity.sa)


class Receipts(py_schema.PySchemaElement):
    receipts: list[Receipt]


def install():
    transaction = sessions.get_system_session().get_or_create_transaction()
    return Receipts.insert_as_schema(transaction, keys.DDHkeyGeneric('//p/living/shopping/receipts'))


install()
