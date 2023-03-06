""" Test Schema manipulations """

import pytest
import typing
import datetime
from core import keys, schemas, pillars, keydirectory, nodes, schema_root


def test_schema_network(ensure_root_node, migros_key_schema, transaction):
    schemas.SchemaNetwork.valid.use()
    return
