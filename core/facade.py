""" Provides a Facade from Core to Frontend """

from __future__ import annotations
import pydantic
import typing
import json
import accept_types


from . import permissions, keys, schemas, nodes, data_nodes, keydirectory, transactions, errors, dapp_attrs, users
from frontend import sessions
from traits import anonymization

import logging

logger = logging.getLogger(__name__)


async def ddh_get(access: permissions.Access, session: sessions.Session, q: str | None = None, accept_header: list[str] | None = None) -> tuple[typing.Any, dict]:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.

        First we get the data (and consent), then we pass it to an enode if an enode is found.

    """
    async with session.get_or_create_transaction(for_user=access.principal) as transaction:
        access.include_mode(permissions.AccessMode.read)
        transaction.add_and_validate(access)

        # fork-independent checks:
        # get schema and key with specifiers:
        schema, access.ddhkey, access.schema_key_split, schema_node, * \
            d = schemas.SchemaContainer.get_node_schema_key(access.ddhkey, transaction)
        mt = check_mimetype_schema(access.ddhkey, schema, accept_header)
        headers = {'Content-Location': str(access.ddhkey), 'Content-Type': mt, }

        match access.ddhkey.fork:
            case keys.ForkType.schema:  # if we ask for schema, we don't need an owner:
                schema = schemas.SchemaContainer.get_sub_schema(access, transaction)
                if schema:
                    trstate = await schema.apply_transformers_to_schema(access, transaction, None)
                    data = trstate.nschema.to_format(schemas.SchemaFormat.json)
                else:
                    data = None  # in case of not found.

            case keys.ForkType.consents:
                access.ddhkey.raise_if_no_owner()
                data = await get_data(access, transaction, q)

            case keys.ForkType.data:
                access.ddhkey.raise_if_no_owner()
                trstate = await schema.apply_transformers(access, transaction, None)
                data = trstate.parsed_data

    return data, headers


async def ddh_put(access: permissions.Access, session: sessions.Session, data: pydantic.Json, q: str | None = None, content_type: str = '*/*', includes_owner: bool = False) -> tuple[typing.Any, dict]:
    """ Service utility to store data.

    """
    async with session.get_or_create_transaction(for_user=access.principal) as transaction:
        access.include_mode(permissions.AccessMode.write)
        transaction.add_and_validate(access)

        if permissions.AccessMode.pseudonym in access.modes:
            # modify access.ddhkey according to real owner:
            await anonymization.resolve_owner(access, transaction)

        # we need a (parent) schema node, even if we put a schema (we accept default parent):
        schema, access.ddhkey, access.schema_key_split, schema_node, * \
            d = schemas.SchemaContainer.get_node_schema_key(access.ddhkey, transaction, default=True)

        headers = {}

        match access.ddhkey.fork:
            case keys.ForkType.schema:
                access.raise_if_not_permitted(schema_node)
                new_schema = typing.cast(schemas.AbstractSchema, data)
                trstate = await schema.apply_transformers_to_schema(access, transaction, new_schema)
                data = trstate.parsed_data

            case keys.ForkType.consents | keys.ForkType.data:

                check_mimetype_schema(access.ddhkey, schema, [content_type], header_field='Content-Type')

                # We need a data node, even for consents, as it carries the consents:
                data_node, d_key_split, remainder = await get_or_create_dnode(access, transaction)
                access.raise_if_not_permitted(data_node)

                match access.ddhkey.fork:
                    case keys.ForkType.consents:
                        consents = permissions.Consents.model_validate_json(data)
                        await data_node.update_consents(access, transaction, remainder, consents)

                    case keys.ForkType.data:
                        trstate = await schema.apply_transformers(access, transaction, data, includes_owner=includes_owner)
                        data = trstate.parsed_data

    return data, headers


async def get_data(access: permissions.Access, transaction: transactions.Transaction, q: str | None = None) -> typing.Any:

    data_node, d_key_split = await keydirectory.NodeRegistry.get_node_async(
        access.ddhkey, nodes.NodeSupports.data, transaction)
    if data_node:
        if access.ddhkey.fork == keys.ForkType.consents:
            access.include_mode(permissions.AccessMode.read)
            *d, consentees, msg = access.raise_if_not_permitted(data_node)
            return data_node.consents
        else:
            data_node = await data_node.ensure_loaded(transaction)
            data_node = typing.cast(data_nodes.DataNode, data_node)
            *d, consentees, msg = access.raise_if_not_permitted(data_node)
            data = await data_node.execute(nodes.Ops.get, access, transaction, d_key_split, None, q)
    else:
        *d, consentees, msg = access.raise_if_not_permitted(await keydirectory.NodeRegistry._get_consent_node_async(
            access.ddhkey, nodes.NodeSupports.data, None, transaction))
        data = {}
    transaction.add_read_consentees({c.id for c in consentees})
    return data


async def get_or_create_dnode(access: permissions.Access, transaction: transactions.Transaction) -> tuple[data_nodes.DataNode, int, keys.DDHkey]:
    data_node, d_key_split = await keydirectory.NodeRegistry.get_node_async(
        access.ddhkey, nodes.NodeSupports.data, transaction)
    if not data_node:

        topkey, remainder = access.ddhkey.split_at(2)
        # there is no node, create it if owner asks for it:
        if access.principal.id in topkey.owners:
            data_node = data_nodes.DataNode(owner=access.principal, key=topkey)
            await data_node.store(transaction)  # put node into directory
        else:  # not owner, we simply say no access to this path
            raise errors.AccessError(f'not authorized to write to {topkey}')
    else:
        data_node = await data_node.ensure_loaded(transaction)
        topkey, remainder = access.ddhkey.split_at(d_key_split)

    data_node = typing.cast(data_nodes.DataNode, data_node)
    return data_node, d_key_split, remainder


async def get_schema(access: permissions.Access, transaction: transactions.Transaction, schemaformat: schemas.SchemaFormat = schemas.SchemaFormat.json) -> pydantic.Json | None:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    schema = schemas.SchemaContainer.get_sub_schema(access, transaction)
    if schema:
        trstate = await schema.apply_transformers_to_schema(access, transaction, None)
        formatted_schema = schema.to_format(schemaformat)
    else:
        formatted_schema = None  # in case of not found.
    return formatted_schema


def check_mimetype_schema(ddhkey: keys.DDHkey, schema: schemas.AbstractSchema, accept_header: list[str] | None, header_field: str = 'Accept') -> str:
    """ raise error if selected schema variant's mimetype is not acceptable in accept_header.
        Design decision:
            We could also look up the variant which  corresponds to the accept_header when no variant is
            specified in the ddhkey. However, we consider this too implicit and potentially surprising.
        return primary contents mimetype 

        header_field: A string mentioning the header field, only used for the error message.
    """
    mt = schema.schema_attributes.mimetypes
    assert mt
    smt = mt.for_fork(ddhkey.fork)  # mimetypes for our ddhkey
    if accept_header:  # check if accept header is supplied.
        amt = ', '.join(accept_header)
        # we provide one mimetype - is it acceptable?
        if not accept_types.get_best_match(amt, smt):
            raise errors.NotAcceptable(f'The mime types {", ".join(smt)} of the selected schema variant {schema.schema_attributes.variant} ' +
                                       f'does not correspond to the {header_field} header media types {amt}; try an alternate schema variant.')
    return smt[0]
