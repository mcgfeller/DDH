""" Root of DDH Schemas, defines top levels down to where DApps link in """

from __future__ import annotations
import pydantic
import datetime
import typing
import logging

from core import keys, schemas, nodes, keydirectory, principals, versions, trait
from schema_formats import py_schema
from frontend import sessions
logger = logging.getLogger(__name__)


def register_schema() -> nodes.SchemaNode:
    """ Register root schema at root node.
        This is preliminary, as the schema is hard-coded.
    """
    assert trait.DefaultTraits.ready, 'traits must be loaded first'
    root = keys.DDHkeyGeneric(keys.DDHkey.Root, fork=keys.ForkType.schema)
    session = sessions.get_system_session()
    transaction = session.get_or_create_transaction()
    root_node, split = keydirectory.NodeRegistry.get_node(
        root, nodes.NodeSupports.schema, transaction)
    if not root_node:
        schema = build_root_schemas()  # obtain static schema
        # for now, give schema read access to everybody
        root_node = nodes.SchemaNode(owner=principals.RootPrincipal, key=root,
                                     consents=schemas.AbstractSchema.get_schema_consents())
        root_node.add_schema(schema)
        keydirectory.NodeRegistry[root] = root_node
        inherit_attributes(schema, transaction)
        schemas.SchemaNetwork.valid.invalidate()  # finished
        logger.info('AbstractSchema Root built')

    return root_node


class TN:
    """ Auxilliary TreeNode, used to build schema tree (only used here) """

    def __init__(self, name: str, sa: schemas.SchemaAttributes | None = None, subscribable: bool = False):
        self.name = name
        self.schema_attributes = sa or schemas.SchemaAttributes(subscribable=subscribable)


def build_root_schemas():
    """ build top of schema tree """
    treetop = [TN('root', schemas.SchemaAttributes(transformers=trait.DefaultTraits.RootTransformers)),
               [TN(''),  # no owner
                [TN('org'),  # organizational tree, next level are org domains
                 [TN('private'),  # for the user him/herself
                  # this is DocSave RIP - cancel validation
                  [TN('documents', schemas.SchemaAttributes(subscribable=True, transformers=trait.DefaultTraits.NoValidation))]
                  ],
                 ],
                   [TN('p'),  # personal tree, next level are data models
                    [TN('family')],
                    [TN('employment'),
                     [TN('salary'),
                      [TN('statements', sa=schemas.SchemaAttributes(requires=schemas.Requires.specific, subscribable=True))]
                      ],
                     ],
                    [TN('education')],
                    [TN('health', schemas.SchemaAttributes(transformers=trait.DefaultTraits.HighestPrivacyTransformers))],
                    [TN('living'),
                     [TN('shopping', subscribable=True),
                      [TN('receipts')]
                      ],
                     ],
                    [TN('finance', sa=schemas.SchemaAttributes(transformers=trait.DefaultTraits.HighPrivacyTransformers)),
                     [TN('tax', subscribable=True),
                      [TN('declaration')]
                      ],
                     [TN('holdings', subscribable=True),
                      [TN('portfolio', sa=schemas.SchemaAttributes(requires=schemas.Requires.specific))]
                      ],
                     ],
                    ],
                ]
               ]

    schema_element = descend_schema(treetop)
    root = py_schema.PySchema(schema_element=schema_element)
    root.schema_attributes.transformers = treetop[0].schema_attributes.transformers  # root schema attributes
    assert root.to_output()  # test schema generation

    return root


def descend_schema(tree: list, parents=()) -> type[schemas.AbstractSchemaElement]:
    """ Descent on our tree representation, returning model """
    key = parents+(tree[0].name,)  # new key, from parents down
    elements = {t[0].name: (descend_schema(t, parents=key), None)
                for t in tree[1:]}  # descend on subtree, build dict of {head_name  : subtree}
    se = py_schema.PySchemaElement.create_from_elements(key, **elements)  # create a model with subtree elements

    if parents and tree[0].name:  # not root and not empty level (=owner level)
        # we need to replace the PySchemaElement by a full Schema and a PySchemaReference to it, so we can set SchemaAttributes
        dkey = keys.DDHkeyGeneric(('', '')+key[2:], fork=keys.ForkType.schema)  # 'root' is '' in key
        se = se.store_as_schema(dkey, tree[0].schema_attributes)
    return se


def inherit_attributes(top: schemas.AbstractSchema, transaction):
    """ recurse on schema to update validations from top-down (cannot do this in descend_schema, because we build
        bottom-up).
    """
    for ref in top.schema_attributes.references.values():
        subkey = keys.DDHkey(ref).ens()
        subschema, *d = schemas.SchemaContainer.get_node_schema_key(subkey, transaction)
        assert subschema is not top, 'schema must not reference itself'
        subschema.schema_attributes.transformers = top.schema_attributes.transformers.merge(
            subschema.schema_attributes.transformers)
        inherit_attributes(subschema, transaction)
    return


register_schema()
