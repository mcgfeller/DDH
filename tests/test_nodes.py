from core import keys, nodes, permissions, schemas, facade, keydirectory, transactions, users
from frontend import sessions
from schema_formats import py_schema


class DummyElement(py_schema.PySchemaElement): ...


def test_nodes():
    schema = py_schema.PySchema(schema_element=DummyElement)
    user = users.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    user2 = users.User(id='2', name='roman', email='roman.stoessel@swisscom.com')
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
        0].schemas.get() is schema
    return


def test_schema_node():
    """ Retrieval of schema and application of get_sub_schema() 
    """
    schema = py_schema.PySchema(schema_element=DummyElement)
    user = users.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction(for_user=user)
    node_s = nodes.SchemaNode(owner=user)
    node_s.add_schema(schema)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health')] = node_s
    ddhkey = keys.DDHkey(key='//p/health/bmi/weight')  # does not exist
    node_s, split = keydirectory.NodeRegistry.get_node(
        ddhkey, nodes.NodeSupports.schema, transaction)
    assert node_s.schemas.get() is schema

    access = permissions.Access(ddhkey=ddhkey, principal=user, modes=[
                                permissions.AccessMode.read])
    assert schemas.SchemaContainer.get_sub_schema(
        access, transaction) is None, 'missing intermediate nodes must not be created'
    assert facade.get_schema(access, transaction) is None  # this should be same in one go.


if __name__ == '__main__':
    test_nodes()
