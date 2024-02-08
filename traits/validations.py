""" Executable Schema Validations """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)
from utils.pydantic_utils import DDHbaseModel, CV
Validations = trait.Transformers  # Synonym, for easier reference, Validations are just Traits


class SchemaValidation(trait.Transformer):
    """ Validation used for Schemas
        GET:

        PUT:
            No shadowing - cannot insert into an existing schema, including into refs
            Reference update
            ref -> update referenced
            schema update -> ref
            uniform schema tree - all references must be in same schema repr 

    """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # Validation is not invoked by mode
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})  # no checks for read
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.schema})
    phase: CV[trait.Phase] = trait.Phase.validation

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        """ in a SchemaValidation, the subject is schema. """
        return


class DataValidation(trait.Transformer):
    """ Validations on data for a schema """
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset()  # Validation is not invoked by mode
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})  # no checks for read
    only_forks: CV[frozenset[keys.ForkType]] = frozenset({keys.ForkType.data})
    phase: CV[trait.Phase] = trait.Phase.validation


class MustReview(SchemaValidation):

    by_roles: frozenset[str] = frozenset()

    def merge(self, other: MustReview) -> typing.Self | None:
        """ return the stronger between self and other validations, creating a new combined 
            validation. Any role is stronger than when no roles are specified. 
        """
        r = super().merge(other)
        if r is not None:
            if r.by_roles != other.by_roles:
                d = self.model_dump()
                if self.may_overwrite:
                    d['by_roles'] = other.by_roles
                else:
                    d['by_roles'] = self.by_roles | other.by_roles
                r = self.__class__(**d)
        return r


class MustHaveSensitivites(SchemaValidation):
    """ This schema must have sensitivity annotations """
    ...


class SchemaExpandReferences(SchemaValidation):
    """ Expand references in schema read """

    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({
        permissions.AccessMode.read, permissions.AccessMode.write})  # check on reads

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, includes_owner: bool = False, **kw):
        trargs.nschema = trargs.nschema.expand_references()
        return


class SchemaMustValidate(SchemaValidation):
    """ This schema must be validated """
    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, includes_owner: bool = False, **kw):
        # TODO
        return


class ParseData(DataValidation):
    """ Data being parsed """

    phase: CV[trait.Phase] = trait.Phase.parse

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, includes_owner: bool = False, **kw):
        try:
            trargs.parsed_data = trargs.nschema.parse(trargs.orig_data)
        except Exception as e:
            raise errors.ParseError(e)
        return


class MustValidate(DataValidation):
    """ Data must be validated """

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, includes_owner: bool = False, **kw):
        owners = trargs.access.original_ddhkey.owners  # original, in case of Pseudonymized
        if len(owners) != 1:
            raise errors.NotSelectable(f"Cannot have multiple owners in key: {','.join(owners)}")
        assert isinstance(trargs.parsed_data, dict)
        if includes_owner:
            if len(trargs.parsed_data) > 1:
                raise errors.NotSelectable('Cannot have multiple owners in data')
            else:
                data = trargs.parsed_data.get(owners[0])
                if data is None:
                    raise errors.NotSelectable(f'No data supplied for owner: {owners[0]}')
        else:
            data = trargs.parsed_data

        remainder = trargs.access.ddhkey.remainder(trargs.access.schema_key_split)

        try:
            trargs.parsed_data = trargs.nschema.validate_data(data, remainder, no_extra=NoExtraElements in traits)
        except errors.DDHerror as e:
            raise
        except Exception as e:
            raise errors.ValidationError(e)

        if isinstance(trargs.parsed_data, DDHbaseModel):  # for PySchemas, we have a model, not a dict
            trargs.parsed_data = trargs.parsed_data.model_dump()  # make dict

        return


class NoExtraElements(DataValidation):
    """ Schema validation will reject extra elements not specified in the schema;
        marker applied by MustValidate
    """
    ...


class LatestVersion(DataValidation):
    """ Data must match latest version of schema or must be upgradable.
    """

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        schema = trargs.nschema
        v_schema = schema.schema_attributes.version  # the version of our schema.
        container = schema.container
        latest = container.get(schema.schema_attributes.variant)  # latest schema version of this variant
        v_latest = latest.schema_attributes.version
        print(f'LatestVersion {v_schema=}, {v_latest=}')
        if v_schema < v_latest:
            # look for upgraders for our variant:
            upgraders: versions.Upgraders = container.upgraders.get(schema.schema_attributes.variant)
            if upgraders:
                try:  # check whether we have a path
                    upgrade_path = upgraders.upgrade_path(v_schema, v_latest)
                except ValueError as e:
                    raise errors.VersionMismatch(
                        f'Version supplied {v_schema} is not latest version {v_latest} and no upgrade path exists ({e}).')
            else:  # no upgraders registered
                raise errors.VersionMismatch(
                    f'Version supplied {v_schema} is not latest version {v_latest} and no upgraders are available')
        return


class UnderSchemaReference(DataValidation):
    """ TODO: Data within schema that includes schema reference only if schema can be expanded """

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        return


# Root validations may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(ParseData(may_overwrite=True), MustValidate(may_overwrite=True), NoExtraElements(
    may_overwrite=True), LatestVersion(may_overwrite=True), UnderSchemaReference(), SchemaMustValidate(), SchemaExpandReferences())

trait.DefaultTraits.NoValidation += trait.Transformers(~MustValidate(may_overwrite=True), ~
                                                       NoExtraElements(may_overwrite=True), UnderSchemaReference(), ~LatestVersion(may_overwrite=True))

trait.DefaultTraits.HighPrivacyTransformers += [MustValidate(), NoExtraElements(), MustHaveSensitivites(), MustReview()]
# Ensure we have a senior reviewer:
trait.DefaultTraits.HighestPrivacyTransformers += trait.DefaultTraits.HighPrivacyTransformers + \
    MustReview(by_roles={'senior'})
