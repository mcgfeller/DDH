""" Test Schema manipulations """

import pytest
from core import keys, nodes, schemas, keydirectory, principals, versions
from frontend import sessions


class DummyElement(schemas.SchemaElement): ...

def test_container():
    sa = schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(0))
    schema = schemas.PySchema(schema_element=DummyElement, schema_attributes=sa)
    sc = schemas.SchemaContainer()
    sc.add(schema)
    sa = schema.schema_attributes
    assert schema is sc.get(variant=sa.variant,version=sa.version)




def test_schema_multiple_versions():
    schema_r0 = schemas.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(0)))
    schema_r1 = schemas.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(1)))
    schema_r3 = schemas.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='rec', variant_usage=schemas.SchemaVariantUsage.recommended, version=versions.Version(3)))

    schema_a2 = schemas.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='alt', variant_usage=schemas.SchemaVariantUsage.supported, version=versions.Version(2)))
    schema_a4 = schemas.PySchema(schema_element=DummyElement, schema_attributes=schemas.SchemaAttributes(
        variant='alt', variant_usage=schemas.SchemaVariantUsage.supported, version=versions.Version(4)))
    user = principals.User(id='1', name='martin', email='martin.gfeller@swisscom.com')

    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction(for_user=user)
    node_s = nodes.SchemaNode(owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health')] = node_s

    node_s.add_schema(schema_r1)
    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r1
    assert str(k) == '//p/health::rec:1'

    node_s.add_schema(schema_r0) # earlier version, no impact
    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r1
    assert str(k) == '//p/health::rec:1'

    node_s.add_schema(schema_r0) # explicit version 0
    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health:::0'),transaction)
    assert s is schema_r0
    assert str(k) == '//p/health::rec:0'

    node_s.add_schema(schema_r3) # later version, becomes default
    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r3
    assert str(k) == '//p/health::rec:3'
    

    node_s.add_schema(schema_a2) # alt version
    node_s.add_schema(schema_a4) # alt version
    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health'),transaction) # alt must be explicit
    assert s is schema_r3 # so it's still preferred
    assert str(k) == '//p/health::rec:3'

    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health::alt'),transaction) # explicit alt, latest version
    assert s is schema_a4
    assert str(k) == '//p/health::alt:4'

    s,k =  schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(key='//p/health::alt:2'),transaction) # explicit alt, earlier version
    assert s is schema_a2
    assert str(k) == '//p/health::alt:2'