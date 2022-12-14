from core import keys, nodes, permissions, schemas, facade, keydirectory, transactions, principals, versions
from frontend import sessions


class DummyElement(schemas.SchemaElement): ...


def test_nodes():
    schema = schemas.PySchema(schema_element=DummyElement)
    user = principals.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    user2 = principals.User(id='2', name='roman', email='roman.stoessel@swisscom.com')
    transaction = transactions.Transaction.create(user)
    node_s = nodes.SchemaNode(owner=user)
    node_s.add_schema(schema)
    node_d = nodes.DataNode(consents=permissions.Consents(
        consents=[permissions.Consent(grantedTo=[user2])]), owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health:schema')] = node_s
    keydirectory.NodeRegistry[keys.DDHkey(key='/mgf/p/health')] = node_d
    ddhkey = keys.DDHkey(key='/mgf/p/health/bmi/weight')
    ddhkey_s = keys.DDHkey('//p/health/bmi/weight:schema')
    assert next(keydirectory.NodeRegistry.get_next_proxy(
        ddhkey, nodes.NodeSupports.data))[0] == node_d.get_proxy()
    assert keydirectory.NodeRegistry.get_node(ddhkey_s, nodes.NodeSupports.schema, transaction)[
        0].schemas.default_schema is schema
    return


def test_schema_node():
    """ Retrieval of schema and application of get_sub_schema() 
    """
    schema = schemas.PySchema(schema_element=DummyElement)
    user = principals.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction(for_user=user)
    node_s = nodes.SchemaNode(owner=user)
    node_s.add_schema(schema)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health')] = node_s
    ddhkey = keys.DDHkey(key='//p/health/bmi/weight')  # does not exist
    node_s, split = keydirectory.NodeRegistry.get_node(
        ddhkey, nodes.NodeSupports.schema, transaction)
    assert node_s.schemas.default_schema is schema
    assert node_s.get_sub_schema(ddhkey, split) is None
    access = permissions.Access(ddhkey=ddhkey, principal=user, modes=[
                                permissions.AccessMode.read])

    assert facade.get_schema(access, transaction) is None  # this should be same in one go.


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
        variant='alt', variant_usage=schemas.SchemaVariantUsage.supported, version=versions.Version(3)))
    user = principals.User(id='1', name='martin', email='martin.gfeller@swisscom.com')

    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction(for_user=user)
    node_s = nodes.SchemaNode(owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health')] = node_s

    node_s.add_schema(schema_r1)
    s,k =  schemas.SchemaContainer.get_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r1
    assert str(k) == '//p/health::rec:1'

    node_s.add_schema(schema_r0) # earlier version, no impact
    s,k =  schemas.SchemaContainer.get_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r1
    assert str(k) == '//p/health::rec:1'

    node_s.add_schema(schema_r0) # explicit version 0
    s,k =  schemas.SchemaContainer.get_schema_key(keys.DDHkey(key='//p/health:::0'),transaction)
    assert s is schema_r0
    assert str(k) == '//p/health::rec:0'

    node_s.add_schema(schema_r3) # later version, becomes default
    s,k =  schemas.SchemaContainer.get_schema_key(keys.DDHkey(key='//p/health'),transaction)
    assert s is schema_r3
    assert str(k) == '//p/health::rec:3'
    


if __name__ == '__main__':
    test_nodes()
