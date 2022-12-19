""" Provides a Facade from Core to Frontend """

from __future__ import annotations
import pydantic
import typing
import json
import accept_types


from . import permissions, keys, schemas, nodes, keydirectory, transactions, errors, dapp_attrs
from frontend import sessions


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


def get_schema(access: permissions.Access, transaction: transactions.Transaction, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> pydantic.Json | None:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    formatted_schema = None  # in case of not found.
    snode, split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.schema, transaction)  # get applicable schema nodes

    if snode:
        access.raise_permitted(_get_consent_node(
            access.ddhkey.without_variant_version(), nodes.NodeSupports.schema, snode, transaction))
        schema = snode.get_sub_schema(access.ddhkey, split)
        if schema:
            formatted_schema = schema.to_format(schemaformat)
    return formatted_schema


async def ddh_get(access: permissions.Access, session: sessions.Session, q: str | None = None, accept_header: list[str] | None = None) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.

        First we get the data (and consent), then we pass it to an enode if an enode is found.

    """

    access.include_mode(permissions.AccessMode.read)
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)

    # fork-independent checks:
    # get schema and key with specifiers:
    schema, access.ddhkey = schemas.SchemaContainer.get_node_schema_key(access.ddhkey, transaction)
    check_mimetype_schema(access.ddhkey, schema, accept_header)

    match access.ddhkey.fork:
        case keys.ForkType.schema:  # if we ask for schema, we don't need an owner:
            schema = get_schema(access, transaction, schemaformat=schemas.SchemaFormat.json)
            return schema

        case keys.ForkType.consents:
            access.ddhkey.raise_if_no_owner()
            consents = await get_data_node(access, transaction, q)
            return consents

        case keys.ForkType.data:
            access.ddhkey.raise_if_no_owner()
            # get data node first
            data = await get_data_node(access, transaction, q)
            # pass data to enode and get result:
            data = await get_enode(access, transaction, data, q)

    return data


async def ddh_put(access: permissions.Access, session: sessions.Session, data: pydantic.Json, q: str | None = None, ) -> typing.Any:
    """ Service utility to store data.

    """
    access.include_mode(permissions.AccessMode.write)
    transaction = session.get_or_create_transaction(for_user=access.principal)
    transaction.add_and_validate(access)

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

    if access.ddhkey.fork == keys.ForkType.data:
        access.raise_permitted(data_node)
        data = json.loads(data)  # make dict
        # first e_node to transform data:
        e_node, e_key_split = keydirectory.NodeRegistry.get_node(
            access.ddhkey.without_owner(), nodes.NodeSupports.execute, transaction)
        if e_node:
            e_node = e_node.ensure_loaded(transaction)
            e_node = typing.cast(nodes.ExecutableNode, e_node)
            data = e_node.execute(nodes.Ops.put, access, transaction, e_key_split, data, q)
        if data:
            data_node.execute(nodes.Ops.put, access, transaction, d_key_split, data, q)

    elif access.ddhkey.fork == keys.ForkType.consents:
        access.include_mode(permissions.AccessMode.write)
        access.raise_permitted(data_node)
        consents = permissions.Consents.parse_raw(data)
        data_node.update_consents(access, transaction, remainder, consents)
    return data


async def get_data_node(access: permissions.Access, transaction: transactions.Transaction, q: str | None = None) -> typing.Any:

    data_node, d_key_split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.data, transaction)
    if data_node:
        if access.ddhkey.fork == keys.ForkType.consents:
            access.include_mode(permissions.AccessMode.read)
            *d, consentees = access.raise_permitted(data_node)
            return data_node.consents
        else:
            data_node = data_node.ensure_loaded(transaction)
            data_node = typing.cast(nodes.DataNode, data_node)
            *d, consentees = access.raise_permitted(data_node)
            data = data_node.execute(nodes.Ops.get, access, transaction, d_key_split, None, q)
    else:
        *d, consentees = access.raise_permitted(_get_consent_node(
            access.ddhkey, nodes.NodeSupports.data, None, transaction))
        data = {}
    transaction.add_read_consentees({c.id for c in consentees})
    return data


async def get_enode(access: permissions.Access, transaction: transactions.Transaction, data: typing.Any, q: str | None = None) -> typing.Any:
    e_node, e_key_split = keydirectory.NodeRegistry.get_node(
        access.ddhkey.without_owner(), nodes.NodeSupports.execute, transaction)
    if e_node:
        e_node = e_node.ensure_loaded(transaction)
        e_node = typing.cast(nodes.ExecutableNode, e_node)
        req = dapp_attrs.ExecuteRequest(
            op=nodes.Ops.get, access=access, transaction=transaction, key_split=e_key_split, data=data, q=q)
        data = await e_node.execute(req)
    return data


def get_schema(access: permissions.Access, transaction: transactions.Transaction, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> pydantic.Json | None:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    formatted_schema = None  # in case of not found.
    snode, split = keydirectory.NodeRegistry.get_node(
        access.ddhkey, nodes.NodeSupports.schema, transaction)  # get applicable schema nodes

    if snode:
        access.raise_permitted(_get_consent_node(
            access.ddhkey.without_variant_version(), nodes.NodeSupports.schema, snode, transaction))
        schema = snode.get_sub_schema(access.ddhkey, split)
        if schema:
            formatted_schema = schema.to_format(schemaformat)
    return formatted_schema


def check_mimetype_schema(ddhkey: keys.DDHkey, schema: schemas.AbstractSchema, accept_header: list[str] | None):
    """ raise error if selected schema variant's mimetype is not acceptable in accept_header.
        Design decision:
            We could also look up the variant which  corresponds to the accept_header when no variant is
            specified in the ddhkey. However, we consider this too implicit and potentially surprising. 
    """
    if accept_header:
        mt = schema.schema_attributes.mimetypes
        assert mt
        smt = mt.for_fork(ddhkey.fork)  # mimetypes for our ddhkey
        amt = ', '.join(accept_header)
        # we provide one mimetype - is it acceptable?
        if not accept_types.get_best_match(amt, [smt]):
            raise errors.NotAcceptable(f'The mime type {smt} of the selected schema variant {schema.schema_attributes.variant} ' +
                                       f'does not correspond to the Accept header media types {amt}; try an alternate schema variant.')
    return
