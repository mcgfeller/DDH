""" Test Schema manipulations """

import pytest
from core import  keys,schemas,schema_root,keydirectory,nodes
from frontend import sessions

@pytest.fixture
def transaction():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    return transaction

@pytest.fixture
def ensure_root_node(transaction):
    root = keys.DDHkey(keys.DDHkey.Root)
    root_node,split = keydirectory.NodeRegistry.get_node(root,nodes.NodeSupports.schema,transaction)
    assert root_node
    return root_node

@pytest.fixture
def json_schema():
    from DApps import MigrosDApp
    s = MigrosDApp.get_apps()[0].get_schemas()
    k,ps = list(s.items())[0]
    js = ps.to_json_schema()
    return k,js

def test_insert_schema(ensure_root_node,json_schema,transaction):
    assert ensure_root_node
    k,schema = json_schema
    # replace_by_schema --> by with JsonSchema
    schemas.AbstractSchema.insert_schema('Migros',k,transaction)



def test_schema(json_schema):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    assert json_schema
    return


if __name__ == '__main__':
    test_schema()