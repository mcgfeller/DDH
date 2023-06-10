""" Executable Schema Restrictions """
from __future__ import annotations

import typing
import copy

from core import (errors,  schemas, assignable, versions, permissions)

Restrictions = assignable.Applicables  # Synonym, for easier reference, Restrictions are just Assignables


class SchemaRestriction(assignable.Applicable):
    """ Restriction used for Schemas """
    supports_modes = frozenset()  # {permissions.AccessMode.write}

    def apply(self,  assignables: assignable.Assignables, schema: schemas.AbstractSchema, access, transaction, subject: schemas.AbstractSchema) -> schemas.AbstractSchema:
        """ in a SchemaRestriction, the subject is schema. """
        return subject


class DataRestriction(assignable.Applicable):
    """ Restrictions on data for a schema """
    supports_modes = frozenset()  # {permissions.AccessMode.write}


class MustReview(SchemaRestriction):

    by_roles: frozenset[str] = frozenset()

    def merge(self, other: MustReview) -> typing.Self | None:
        """ return the stronger between self and other restrictions, creating a new combined 
            restriction. Any role is stronger than when no roles are specified. 
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


class MustHaveSensitivites(SchemaRestriction):
    """ This schema must have sensitivity annotations """
    ...


class MustValidate(DataRestriction):
    """ Data must be validated """

    def apply(self,  assignables: assignable.Assignables, schema, access, transaction, data: assignable.Tsubject) -> assignable.Tsubject:
        remainder = access.ddhkey.remainder(access.schema_key_split)
        for owner, d in data.items():  # loop through owners, as schema doesn't contain owner
            try:
                data[owner] = schema.validate_data(d, remainder, no_extra=NoExtraElements in assignables)
            except errors.DDHerror as e:
                raise
            except Exception as e:
                raise errors.ValidationError(e)

        return data


class NoExtraElements(DataRestriction):
    """ Schema validation will reject extra elements not specified in the schema;
        marker applied by MustValidate
    """
    ...


class LatestVersion(DataRestriction):
    """ Data must match latest version of schema or must be upgradable.
    """

    def apply(self,  assignables: assignable.Assignables, schema, access, transaction, data: assignable.Tsubject) -> assignable.Tsubject:
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
        return data


class UnderSchemaReference(DataRestriction):
    """ TODO: data under schema reference only if schema reprs are compatible """

    def apply(self,  assignables: assignable.Assignables, schema, access, transaction, data: assignable.Tsubject) -> assignable.Tsubject:
        return data


NoRestrictions = Restrictions()
# Root restrictions may be overwritten:
RootRestrictions = Restrictions(MustValidate(may_overwrite=True), NoExtraElements(
    may_overwrite=True), LatestVersion(may_overwrite=True), UnderSchemaReference())
NoValidation = Restrictions(~MustValidate(may_overwrite=True), ~
                            NoExtraElements(may_overwrite=True), UnderSchemaReference(), ~LatestVersion(may_overwrite=True))
HighPrivacyRestrictions = RootRestrictions + [MustValidate(), NoExtraElements(), MustHaveSensitivites(), MustReview()]
# Ensure we have a senior reviewer:
HighestPrivacyRestrictions = HighPrivacyRestrictions+MustReview(by_roles={'senior'})
