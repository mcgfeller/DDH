""" Fixtures for testing with actual microservices """

import typing
import pytest

from core import keys, schemas, pillars, keydirectory, nodes, schema_root, dapp_proxy, dapp_attrs, permissions
from schema_formats import py_schema
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
def migros_key_schema(transaction):
    """ retrieve Migros Schema"""
    from DApps import MigrosDApp
    app = MigrosDApp.get_apps()[0]
    s = app.get_schemas()
    k, schema = list(s.items())[0]

    # register in Schema Node, so tests can retrieve it:
    dapp_proxy.DAppProxy.register_schema(k, schema, app.owner, transaction)
    return k, schema


@pytest.fixture(scope="session")
def migrosddhkey():
    return keys.DDHkey(key="/mgf/org/migros.ch")


@pytest.fixture(scope="session")
def migros_data(migrosddhkey, transaction):
    """ retrieve Migros Data - useful for mocks """
    from DApps import MigrosDApp
    app = MigrosDApp.get_apps()[0]
    session = sessions.get_system_session()
    access = permissions.Access(op=permissions.Operation.get, ddhkey=migrosddhkey,
                                principal=session.user, modes={permissions.AccessMode.read}, byDApp=session.dappid)
    h, s = migrosddhkey.split_at(4)
    d = app.get_data(s, access, None)
    return d
