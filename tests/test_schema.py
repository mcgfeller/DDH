""" Test Schema manipulations """

import pytest
from core import keys, schemas, pillars, keydirectory, nodes, schema_root
from frontend import sessions
from schema_formats import py_schema


@pytest.fixture
def transaction():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    return transaction


@pytest.fixture
def ensure_root_node(transaction):
    root = keys.DDHkey(keys.DDHkey.Root)
    root_node, split = keydirectory.NodeRegistry.get_node(root, nodes.NodeSupports.schema, transaction)
    assert root_node
    return root_node


@pytest.fixture
def migros_key_schema():
    """ retrieve Migros Schema"""
    from DApps import MigrosDApp
    s = MigrosDApp.get_apps()[0].get_schemas()
    k, ps = list(s.items())[0]
    return k, ps


def test_container(migros_key_schema):
    k, schema = migros_key_schema
    json_schema = schema.to_json_schema()
    sc = schemas.SchemaContainer()
    sc.add(schema)
    sa = schema.schema_attributes
    assert schema is sc.get(variant=sa.variant, version=sa.version)


def test_insert_schema(ensure_root_node, migros_key_schema, transaction):
    assert ensure_root_node
    k, schema = migros_key_schema
    json_schema = schema.to_json_schema()
    # replace_by_schema --> by with JsonSchema
    py_schema.PySchema.insert_schema('Migros', k, transaction)


def test_schema_to_json(migros_key_schema):
    """ test conversion to JSON """
    json_schema = migros_key_schema[1].to_json_schema()
    assert json_schema
    return


def test_schema_iterator(migros_key_schema):
    assert [se for se in migros_key_schema[1]]
    json_schema = migros_key_schema[1].to_json_schema()
    # assert [se for se in json_schema]
