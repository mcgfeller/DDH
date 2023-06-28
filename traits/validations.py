""" Executable Schema Validations """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, trait, versions, permissions, keys, nodes, keydirectory, dapp_attrs)

Validations = trait.Transformers  # Synonym, for easier reference, Validations are just Traits


class SchemaValidation(trait.Transformer):
    """ Validation used for Schemas """
    supports_modes = frozenset()  # Validation is not invoked by mode
    only_modes = {permissions.AccessMode.write}  # no checks for read
    only_forks = {keys.ForkType.schema}
    phase = trait.Phase.validation

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        """ in a SchemaValidation, the subject is schema. """
        return


class DataValidation(trait.Transformer):
    """ Validations on data for a schema """
    supports_modes = frozenset()  # Validation is not invoked by mode
    only_modes = {permissions.AccessMode.write}  # no checks for read
    only_forks = {keys.ForkType.data}
    phase = trait.Phase.validation


class MustReview(SchemaValidation):

    by_roles: frozenset[str] = frozenset()

    def merge(self, other: MustReview) -> typing.Self | None:
        """ return the stronger between self and other validations, creating a new combined 
            validation. Any role is stronger than when no roles are specified. 
        """
        r = super().merge(other)
        if r is not None:
            if r.by_roles != other.by_roles:
                d = self.dict()
                if self.may_overwrite:
                    d['by_roles'] = other.by_roles
                else:
                    d['by_roles'] = self.by_roles | other.by_roles
                r = self.__class__(**d)
        return r


class MustHaveSensitivites(SchemaValidation):
    """ This schema must have sensitivity annotations """
    ...


class ParseData(DataValidation):
    """ Data being parsed """

    phase = trait.Phase.parse

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, omit_owner: bool = True, **kw):
        try:
            parsed = trargs.nschema.parse(trargs.orig_data)
        except Exception as e:
            raise errors.ParseError(e)
        if omit_owner:  # add owner if omitted in data
            parsed = {str(trargs.access.principal): parsed}
        trargs.parsed_data = parsed
        return


class MustValidate(DataValidation):
    """ Data must be validated """

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, omit_owner: bool = True, **kw):
        remainder = trargs.access.ddhkey.remainder(trargs.access.schema_key_split)
        assert isinstance(trargs.parsed_data, dict)
        for owner, d in trargs.parsed_data.items():  # loop through owners, as schema doesn't contain owner
            try:
                trargs.parsed_data[owner] = trargs.nschema.validate_data(
                    d, remainder, no_extra=NoExtraElements in traits)
            except errors.DDHerror as e:
                raise
            except Exception as e:
                raise errors.ValidationError(e)

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
    """ TODO: data under schema reference only if schema reprs are compatible """

    async def apply(self,  traits: trait.Traits, trargs: trait.TransformerArgs, **kw):
        return


# Root validations may be overwritten:
trait.DefaultTraits.RootTransformers += trait.Transformers(ParseData(may_overwrite=True), MustValidate(may_overwrite=True), NoExtraElements(
    may_overwrite=True), LatestVersion(may_overwrite=True), UnderSchemaReference())

trait.DefaultTraits.NoValidation += trait.Transformers(~MustValidate(may_overwrite=True), ~
                                                       NoExtraElements(may_overwrite=True), UnderSchemaReference(), ~LatestVersion(may_overwrite=True))

trait.DefaultTraits.HighPrivacyTransformers += [MustValidate(), NoExtraElements(), MustHaveSensitivites(), MustReview()]
# Ensure we have a senior reviewer:
trait.DefaultTraits.HighestPrivacyTransformers += trait.DefaultTraits.HighPrivacyTransformers + \
    MustReview(by_roles={'senior'})
