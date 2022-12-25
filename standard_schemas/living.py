""" Standard schemas living """
from __future__ import annotations

import datetime
import typing

import pydantic

from core import schemas,keys,nodes, principals, keydirectory, errors
from frontend import sessions 


class Receipt(schemas.SchemaElement):

    buyer: str = pydantic.Field(sensitivity=schemas.Sensitivity.eid)
    article: str
    quantity: float = 1.0
    amount: float = 0.0
    when: datetime.datetime = pydantic.Field(sensitivity=schemas.Sensitivity.sa)
    where: str = pydantic.Field(sensitivity=schemas.Sensitivity.sa)



def install_schema(transaction, ddhkey : keys.DDHkey, sel: typing.Type[schemas.SchemaElement],schema_attributes: schemas.SchemaAttributes | None = None):
    ddhkey = ddhkey.ensure_fork(keys.ForkType.schema)
    schemaref = sel.replace_by_schema(ddhkey,schema_attributes=schema_attributes)
    pkey = ddhkey.up() 
    if not pkey:
        raise errors.NotFound('no parent node')
    upnode, split = keydirectory.NodeRegistry.get_node(
        pkey, nodes.NodeSupports.schema, transaction)

    upnode = typing.cast(nodes.SchemaNode, upnode)
    # TODO: We should check some ownership permission here!
    parent = upnode.get_sub_schema(pkey, split, create=False)  # create missing segments
    assert parent  # must exist because create=True

    # now insert our schema into the parent's:
    parent.add_fields({ddhkey[-1]: (schemaref, None)})


    return parent

def install():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    return install_schema(transaction,keys.DDHkey('//p/living/shopping/receipts'),Receipt)

install()