""" Fixtures for testing with actual microservices """

import pytest

from core import keys, schemas, pillars, keydirectory, nodes, schema_root
from frontend import sessions


@pytest.fixture(scope="session")
def transaction():
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    return transaction


@pytest.fixture(scope="session")
def ensure_root_node(transaction):
    root = keys.DDHkey(keys.DDHkey.Root)
    root_node, split = keydirectory.NodeRegistry.get_node(root, nodes.NodeSupports.schema, transaction)
    assert root_node
    return root_node


@pytest.fixture(scope="session")
def migros_key_schema():
    """ retrieve Migros Schema"""
    from DApps import MigrosDApp
    s = MigrosDApp.get_apps()[0].get_schemas()
    k, ps = list(s.items())[0]
    return k, ps
