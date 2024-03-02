""" Test Schema manipulations """

import pytest
from core import keys, nodes, schemas, keydirectory, users, versions, pillars
from frontend import sessions
from schema_formats import py_schema


class DummyElement(py_schema.PySchemaElement): ...


def test_container():
    sa = schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(0))
    schema = py_schema.PySchema(schema_element=DummyElement, schema_attributes=sa)
    sc = schemas.SchemaContainer()
    k = keys.DDHkey('/org/test/test_schema:schema')
    sc.add(k, schema)
    sa = schema.schema_attributes
    assert schema is sc.get(variant=sa.variant, version=sa.version)


def test_schema_multiple_versions(node_registry):
    schema_r0 = py_schema.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(0)))
    schema_r1 = py_schema.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(1)))
    schema_r3 = py_schema.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(3)))

    schema_a2 = py_schema.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='alt', variant_usage=schemas.SchemaVariantUsage.supported, version=versions.Version(2)))
    schema_a4 = py_schema.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='alt', variant_usage=schemas.SchemaVariantUsage.supported, version=versions.Version(4)))
    user = users.User(id='1', name='martin', email='martin.gfeller@swisscom.com')

    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction()
    node_s = nodes.SchemaNode(owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health')] = node_s

    node_s.add_schema(schema_r1)
    s, k, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health:schema'), transaction)
    assert s is schema_r1
    assert str(k) == '//p/health:schema:rec:1'

    node_s.add_schema(schema_r0)  # earlier version, no impact
    s, k, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health:schema'), transaction)
    assert s is schema_r1
    assert str(k) == '//p/health:schema:rec:1'

    node_s.add_schema(schema_r0)  # explicit version 0
    s, k, *d = schemas.SchemaContainer.get_node_schema_key(
        keys.DDHkey(key='//p/health:schema::0'), transaction)
    assert s is schema_r0
    assert str(k) == '//p/health:schema:rec:0'

    node_s.add_schema(schema_r3)  # later version, becomes default
    s, k, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health:schema'), transaction)
    assert s is schema_r3
    assert str(k) == '//p/health:schema:rec:3'

    node_s.add_schema(schema_a2)  # alt version
    node_s.add_schema(schema_a4)  # alt version
    s, k, *d = schemas.SchemaContainer.get_node_schema_key(
        keys.DDHkey(key='//p/health:schema'), transaction)  # alt must be explicit
    assert s is schema_r3  # so it's still preferred
    assert str(k) == '//p/health:schema:rec:3'

    s, k, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(
        key='//p/health:schema:alt'), transaction)  # explicit alt, latest version
    assert s is schema_a4
    assert str(k) == '//p/health:schema:alt:4'

    s, k, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(
        key='//p/health:schema:alt:2'), transaction)  # explicit alt, earlier version
    assert s is schema_a2
    assert str(k) == '//p/health:schema:alt:2'

    # test number of schemas fulfilling keys and versions:
    assert 4 == len(list(node_s.container.fullfills(keys.DDHkey(key='//p/health:schema'), versions.NoConstraint)))
    assert 2 == len(list(node_s.container.fullfills(keys.DDHkey(
        key='//p/health:schema'), versions.VersionConstraint('>2'))))
    assert 1 == len(list(node_s.container.fullfills(keys.DDHkey(key='//p/health:schema'),
                    versions.VersionConstraint('>4')))), 'only schema with unspecified version'
    assert 3 == len(list(node_s.container.fullfills(keys.DDHkey(
        key='//p/health:schema:alt'), versions.VersionConstraint('>1'))))
    return
