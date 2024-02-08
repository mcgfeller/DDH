""" Insert .org keys and owners, so owners have permissions to insert schemas.
"""
from __future__ import annotations

from core import schemas, keys, nodes, principals, keydirectory, errors, permissions
from frontend import sessions
from schema_formats import py_schema

# org and seq of users allowed to write them.
orgs: dict[str, tuple[str, ...]] = {
    'migros.ch': ('migros',),
    'coop.ch': ('coop',),
    'sbb.ch': ('sbb',),
    'swisscom.com': ('swisscom',),
    'credit-suisse.com': ('cs',),

}


def install():
    """ Install SchemaNodes for each .org, and with write consent to their owners.
        We don't actually have a schema yet, so we use a dummy schema.
    """
    transaction = sessions.get_system_session().get_or_create_transaction()
    for k, owners in orgs.items():
        key = keys.DDHkeyVersioned0('//org/'+k+':schema')
        # convert owners to principals:
        owners = [o if isinstance(o, principals.Principal) else principals.Principal(
            id=principals.common_ids.PrincipalId(o)) for o in owners]
        write_consent = permissions.Consent(grantedTo=owners, withModes={permissions.AccessMode.write})
        # combine with default consents:
        consents = permissions.Consents(consents=schemas.AbstractSchema.get_schema_consents().consents+[write_consent])
        # SchemaNode has single owner.
        s_node = nodes.SchemaNode(owner=owners[0], consents=consents)
        keydirectory.NodeRegistry[key] = s_node
        # create and add dummy schema:
        dummy_schema = py_schema.PySchema(schema_element=py_schema.PySchemaElement)
        s_node.add_schema(dummy_schema)
    # transaction.commit()


install()
