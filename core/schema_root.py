""" Root of DDH Schemas, defines top levels down to where DApps link in """

from __future__ import annotations
import pydantic
import datetime
import typing
import logging

from core import keys, schemas, nodes, keydirectory, principals, versions
from schema_formats import py_schema
from frontend import sessions
logger = logging.getLogger(__name__)


def register_schema() -> nodes.SchemaNode:
    """ Register root schema at root node. 
        This is preliminary, as the schema is hard-coded.
    """
    root = keys.DDHkeyGeneric(keys.DDHkey.Root, fork=keys.ForkType.schema)
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    root_node, split = keydirectory.NodeRegistry.get_node(
        root, nodes.NodeSupports.schema, transaction)
    if not root_node:
        schema = build_root_schemas()  # obtain static schema
        # for now, give schema read access to everybody
        root_node = nodes.SchemaNode(owner=principals.RootPrincipal,
                                     consents=schemas.AbstractSchema.get_schema_consents())
        keydirectory.NodeRegistry[root] = root_node
        root_node.add_schema(schema)
        schemas.SchemaNetwork.valid.invalidate()  # finished
        logger.info('AbstractSchema Root built')
    return root_node


def build_root_schemas():
    """ build top of schema tree """
    treetop = ['root',
               ['',  # no owner
                ['org',  # organizational tree, next level are org domains
                 ['private',  # for the user him/herself
                  ['documents']
                  ],
                 ],
                   ['p',  # personal tree, next level are data models
                    ['family'],
                    ['employment',
                     ['salary',
                      ['statements']
                      ],
                     ],
                    ['education'],
                    ['health'],
                    ['living',
                     ['shopping',
                      ['receipts']
                      ],
                     ],
                    ['finance',
                     ['tax',
                      ['declaration']
                      ],
                     ['holdings',
                      ['portfolio']
                      ],
                     ],
                    ],
                ]
               ]

    # elements with SchemaAttributes:
    attributes = {
        ('root', '', 'p', 'employment', 'salary', 'statements'): schemas.SchemaAttributes(requires=schemas.Requires.specific),
        ('root', '', 'p', 'finance', 'holdings', 'portfolio'): schemas.SchemaAttributes(requires=schemas.Requires.specific),
    }
    root = py_schema.PySchema(schema_element=descend_schema(treetop, attributes))
    assert root.schema_element.schema_json()
    return root


def descend_schema(tree: list, schema_attributes: dict, parents=()) -> type[schemas.AbstractSchemaElement]:
    """ Descent on our tree representation, returning model """
    key = parents+(tree[0],)  # new key, from parents down
    elements = {t[0]: (descend_schema(t, schema_attributes, parents=key), None)
                for t in tree[1:]}  # descend on subtree, build dict of {head_name  : subtree}
    se = py_schema.PySchemaElement.create_from_elements(key, **elements)  # create a model with subtree elements
    if sa := schema_attributes.get(key):  # SchemaAttributes here?
        # we need to replace the PySchemaElement by a full Schema and a PySchemaReference to it
        dkey = keys.DDHkeyGeneric(('', '')+key[2:], fork=keys.ForkType.schema)  # 'root' is '' in key
        se = se.store_as_schema(dkey, sa)
    return se


register_schema()
