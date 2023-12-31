""" Test Schema manipulations """

import pytest
import typing
import datetime
from core import keys, schemas, pillars, keydirectory, nodes, schema_root
from frontend import sessions
from schema_formats import py_schema


def check_schema(schema):
    """ check if schema is valid """
    assert schema.schema_element.schema()
    assert schema.schema_element.model_json_schema()
    assert [se for se in schema]


def test_container(migros_key_schema):
    k, schema = migros_key_schema
    json_schema = schema.to_json_schema()
    sc = schemas.SchemaContainer()
    sc.add(k, schema)
    sa = schema.schema_attributes
    assert schema is sc.get(variant=sa.variant, version=sa.version)


def test_insert_schema_ref(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    # store_as_schema --> by with JsonSchema
    parent, split = schemas.AbstractSchema.get_parent_schema(transaction, k)
    parent.insert_schema_ref(transaction, k, split)
    check_schema(schema)


def test_insert_py_schemaelement(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    schema[keys.DDHkey('garantie')] = Garantie
    assert schema[keys.DDHkey('garantie')] is Garantie
    check_schema(schema)


def test_replace_py_schemaelement(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    schema[keys.DDHkey('receipts/Produkt/garantie')] = Garantie
    assert schema[keys.DDHkey('receipts/Produkt/garantie')] is Garantie
    check_schema(schema)


def test_insert_py_schemaelement_intermediate(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    schema.__setitem__(keys.DDHkey('products/garantie'), Garantie, create_intermediate=True)
    assert schema[keys.DDHkey('products/garantie')] is Garantie
    check_schema(schema)


def test_insert_py_reference(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    parent, split = schemas.AbstractSchema.get_parent_schema(transaction, k)
    s = Garantie.store_as_schema(k+'refgarantie', parent=parent)
    schema[keys.DDHkey('Garantie')] = s
    check_schema(schema)


def test_schema_to_json(migros_key_schema):
    """ test conversion to JSON """
    json_schema = migros_key_schema[1].to_json_schema()
    assert json_schema
    return


def test_schema_iterator(migros_key_schema):
    assert [se for se in migros_key_schema[1]]
    json_schema = migros_key_schema[1].to_json_schema()
    # assert [se for se in json_schema]


class Garantie(py_schema.PySchemaElement):
    """ Details of a product """
    issuer: str = 'Migros'
    garantie_bis: datetime.date
