import pytest

from core import keys, nodes, data_nodes, permissions, schemas, facade, keydirectory, transactions, users
from frontend import sessions
from schema_formats import py_schema


class DummyElement(py_schema.PySchemaElement): ...


def test_nodes(node_registry):
    schema = py_schema.PySchema(schema_element=DummyElement)
    user = users.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    user2 = users.User(id='2', name='roman', email='roman.stoessel@swisscom.com')
    transaction = transactions.Transaction.create(user)
    node_s = nodes.SchemaNode(owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health:schema')] = node_s
    node_s.add_schema(schema)
    # add consent:
    node_d = data_nodes.DataNode(consents=permissions.Consents(
        consents=[permissions.Consent(grantedTo=[user2])]), owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='/mgf/p/health')] = node_d
    ddhkey = keys.DDHkey(key='/mgf/p/health/bmi/weight')
    ddhkey_s = keys.DDHkey('//p/health/bmi/weight:schema')
    assert next(keydirectory.NodeRegistry.get_next_proxy(
        ddhkey, nodes.NodeSupports.data))[0] == node_d.get_proxy()
    assert keydirectory.NodeRegistry.get_node(ddhkey_s, nodes.NodeSupports.schema, transaction)[
        0].container.get() is schema
    return


@pytest.mark.asyncio
async def test_schema_node(node_registry):
    """ Retrieval of schema and application of get_sub_schema() 
    """
    schema = py_schema.PySchema(schema_element=DummyElement)
    user = users.User(id='1', name='martin', email='martin.gfeller@swisscom.com')
    session = sessions.Session(token_str='test_session', user=user)
    transaction = session.get_or_create_transaction(for_user=user)
    node_s = nodes.SchemaNode(owner=user)
    keydirectory.NodeRegistry[keys.DDHkey(key='//p/health:schema')] = node_s
    node_s.add_schema(schema)
    ddhkey = keys.DDHkey(key='//p/health/bmi/weight:schema')  # does not exist
    node_s, split = keydirectory.NodeRegistry.get_node(
        ddhkey, nodes.NodeSupports.schema, transaction)
    assert node_s.container.get() is schema

    access = permissions.Access(ddhkey=ddhkey, principal=user, modes=[
                                permissions.AccessMode.read])

    parent_schema, access.ddhkey, split, snode, *d = schemas.SchemaContainer.get_node_schema_key(
        access.ddhkey, transaction)
    remainder = access.ddhkey.remainder(split)
    schema_element = parent_schema.__getitem__(remainder, create_intermediate=False)
    assert schema_element is None, 'missing intermediate nodes must not be created'
    assert (await facade.ddh_get(access, session))[0] is None  # this should be same in one go.
