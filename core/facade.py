""" Provides a Facade from Core to Frontend """

from __future__ import annotations
import pydantic
import typing
import json
import accept_types


from . import permissions, keys, schemas, nodes, keydirectory, transactions, errors, dapp_attrs
from frontend import sessions

import logging

logger = logging.getLogger(__name__)


async def ddh_get(access: permissions.Access, session: sessions.Session, q: str | None = None, accept_header: list[str] | None = None) -> tuple[typing.Any, dict]:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.

        First we get the data (and consent), then we pass it to an enode if an enode is found.

    """

    access.include_mode(permissions.AccessMode.read)
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)

    # fork-independent checks:
    # get schema and key with specifiers:
    schema, access.ddhkey, *d = schemas.SchemaContainer.get_node_schema_key(access.ddhkey, transaction)
    mt = check_mimetype_schema(access.ddhkey, schema, accept_header)
    headers = {'Content-Location': str(access.ddhkey), 'Content-Type': mt, }

    match access.ddhkey.fork:
        case keys.ForkType.schema:  # if we ask for schema, we don't need an owner:
            data = get_schema(access, transaction, schemaformat=schemas.SchemaFormat.json)

        case keys.ForkType.consents:
            access.ddhkey.raise_if_no_owner()
            data = await get_data(access, transaction, q)

        case keys.ForkType.data:
            access.ddhkey.raise_if_no_owner()
            # get data node first
            data = await get_data(access, transaction, q)
            # pass data to enode and get result:
            data = await get_enode(nodes.Ops.get, access, transaction, data, q)
            data = schema.after_data_read(access, transaction, data)

    return data, headers


async def ddh_put(access: permissions.Access, session: sessions.Session, data: pydantic.Json, q: str | None = None, accept_header: list[str] | None = None) -> tuple[typing.Any, dict]:
    """ Service utility to store data.

    """
    access.include_mode(permissions.AccessMode.write)
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)

    # We need a data node, even for a schema, as it carries the consents:
    data_node, d_key_split, remainder = get_or_create_dnode(access, transaction)
    access.raise_if_not_permitted(data_node)
    headers = {}

    match access.ddhkey.fork:
        case keys.ForkType.schema:
            schema = typing.cast(schemas.AbstractSchema, data)
            put_schema(access, transaction, schema)

        case keys.ForkType.consents | keys.ForkType.data:
            schema, access.ddhkey, *d = schemas.SchemaContainer.get_node_schema_key(access.ddhkey, transaction)
            check_mimetype_schema(access.ddhkey, schema, accept_header)

            match access.ddhkey.fork:
                case keys.ForkType.consents:
                    consents = permissions.Consents.parse_raw(data)
                    data_node.update_consents(access, transaction, remainder, consents)

                case keys.ForkType.data:
                    # TODO: Checks
                    # + Schema exists for data version
                    # - non-latest version only if upgrade exists (consider again: New Schema may make everything fail)
                    # - Data within schema that includes schema reference only if schema can be expanded
                    data = json.loads(data)  # make dict
                    # check data against Schema
                    data = schema.after_data_read(access, transaction, data)

                    # first e_node to transform data:
                    data = await get_enode(nodes.Ops.put, access, transaction, data, q)
                    if data:
                        data_node.execute(nodes.Ops.put, access, transaction, d_key_split, data, q)

    return data, headers


async def get_data(access: permissions.Access, transaction: transactions.Transaction, q: str | None = None) -> typing.Any:

    data_node, d_key_split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.data, transaction)
    if data_node:
        if access.ddhkey.fork == keys.ForkType.consents:
            access.include_mode(permissions.AccessMode.read)
            *d, consentees, msg = access.raise_if_not_permitted(data_node)
            return data_node.consents
        else:
            data_node = data_node.ensure_loaded(transaction)
            data_node = typing.cast(nodes.DataNode, data_node)
            *d, consentees, msg = access.raise_if_not_permitted(data_node)
            data = data_node.execute(nodes.Ops.get, access, transaction, d_key_split, None, q)
    else:
        *d, consentees, msg = access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
            access.ddhkey, nodes.NodeSupports.data, None, transaction))
        data = {}
    transaction.add_read_consentees({c.id for c in consentees})
    return data


def get_or_create_dnode(access: permissions.Access, transaction: transactions.Transaction) -> tuple[nodes.DataNode, int, keys.DDHkey]:
    data_node, d_key_split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.data, transaction)
    if not data_node:

        topkey, remainder = access.ddhkey.split_at(2)
        # there is no node, create it if owner asks for it:
        if access.principal.id in topkey.owners:
            data_node = nodes.DataNode(owner=access.principal, key=topkey)
            data_node.store(transaction)  # put node into directory
        else:  # not owner, we simply say no access to this path
            raise errors.AccessError(f'not authorized to write to {topkey}')
    else:
        data_node = data_node.ensure_loaded(transaction)
        topkey, remainder = access.ddhkey.split_at(d_key_split)

    data_node = typing.cast(nodes.DataNode, data_node)
    return data_node, d_key_split, remainder


async def get_enode(op: nodes.Ops, access: permissions.Access, transaction: transactions.Transaction, data: typing.Any, q: str | None = None) -> typing.Any:
    e_node, e_key_split = keydirectory.NodeRegistry.get_node(
        access.ddhkey.without_owner(), nodes.NodeSupports.execute, transaction)
    if e_node:
        e_node = e_node.ensure_loaded(transaction)
        e_node = typing.cast(nodes.ExecutableNode, e_node)
        req = dapp_attrs.ExecuteRequest(
            op=op, access=access, transaction=transaction, key_split=e_key_split, data=data, q=q)
        data = await e_node.execute(req)
    return data


def get_schema(access: permissions.Access, transaction: transactions.Transaction, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> pydantic.Json | None:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    schema = schemas.SchemaContainer.get_sub_schema(access, transaction)
    if schema:
        schema = schema.after_schema_read(access, transactions)
        formatted_schema = schema.to_format(schemaformat)
    else:
        formatted_schema = None  # in case of not found.
    return formatted_schema


def put_schema(access: permissions.Access, transaction: transactions.Transaction, schema: schemas.AbstractSchema):
    """ Service utility to store a Schema.
        TODO: WIP
    """
    snode, split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.schema, transaction)  # get applicable schema nodes

    if snode:
        access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
            access.ddhkey.without_variant_version(), nodes.NodeSupports.schema, snode, transaction))
        schema = schema.before_schema_write(access, transactions)
        # schema = snode.get_sub_schema(access.ddhkey, split) # TODO!
    return


def check_mimetype_schema(ddhkey: keys.DDHkey, schema: schemas.AbstractSchema, accept_header: list[str] | None) -> str:
    """ raise error if selected schema variant's mimetype is not acceptable in accept_header.
        Design decision:
            We could also look up the variant which  corresponds to the accept_header when no variant is
            specified in the ddhkey. However, we consider this too implicit and potentially surprising.
        return primary contens mimetype 
    """
    mt = schema.schema_attributes.mimetypes
    assert mt
    smt = mt.for_fork(ddhkey.fork)  # mimetypes for our ddhkey
    if accept_header:  # check if accept header is supplied.
        amt = ', '.join(accept_header)
        # we provide one mimetype - is it acceptable?
        if not accept_types.get_best_match(amt, smt):
            raise errors.NotAcceptable(f'The mime types {", ".join(smt[0])} of the selected schema variant {schema.schema_attributes.variant} ' +
                                       f'does not correspond to the Accept header media types {amt}; try an alternate schema variant.')
    return smt[0]


def _get_consent_node(ddhkey: keys.DDHkey, support: nodes.NodeSupports, node: nodes.Node | None, transaction: transactions.Transaction) -> nodes.Node | None:
    """ get consents, from current node or from its parent """
    if node and node.has_consents():
        cnode = node
    else:
        cnode, d = keydirectory.NodeRegistry.get_node(
            ddhkey, support, transaction, condition=nodes.Node.has_consents)
        if not cnode:  # means that upper nodes don't have consent
            cnode = node
    return cnode
