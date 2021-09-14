""" Root of DDH Schemas, defines top levels down to where DApps link in """

from __future__ import annotations
import pydantic 
import datetime
import typing
import logging

from core import keys,permissions,schemas,nodes,dapp,keydirectory
logger = logging.getLogger(__name__)

def check_registry() -> nodes.Node:
    """ Register root schema at root node. 
        This is preliminary, as the schema is hard-coded.
    """
    root = keys.DDHkey(keys.DDHkey.Root)
    root_node,split = keydirectory.NodeRegistry.get_node(root,nodes.NodeType.nschema)
    if not root_node:
        schema = build_root_schemas() # obtain static schema
        # for now, give schema read access to everybody
        consents = permissions.Consents(consents=[permissions.Consent(grantedTo=[permissions.AllPrincipal],withModes={permissions.AccessMode.schema_read})]) 
        root_node = nodes.Node(owner=permissions.RootPrincipal,schema=schema,consents=consents)
        keydirectory.NodeRegistry[root] = root_node
    logger.info('Schema Root built')
    return root_node 

def build_root_schemas():
    """ build top of schema tree """
    treetop = ['root',
        ['', # no owner
            ['org', # organizational tree, next level are org domains
                ['private'], # for the user him/herself
                    ['documents']
            ],
            ['p', # personal tree, next level are data models
                ['family'],
                ['employment'],
                ['education'],
                ['health'],
                ['living',
                    ['shopping',
                        ['receipts']
                    ],
                ],
                ['finance'],
            ],
        ]
    ]
    root = schemas.PySchema(schema_element=descend_schema(treetop))
    assert root.schema_element.schema_json()
    return root


def descend_schema(tree : list,parents=()) -> pydantic.BaseModel:
    """ Descent on our tree representation, returning model """
    key = parents+(tree[0],) # new key, from parents down
    elements = {t[0]: (descend_schema(t,parents=key),None) for t in tree[1:]} # descend on subtree, build dict of {head_name  : subtree}
    s = pydantic.create_model('_'.join(key), __base__=schemas.SchemaElement, **elements) # create a model with subtree elements
    return s

check_registry()
