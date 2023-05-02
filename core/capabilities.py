""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel

from . import (errors, versions, permissions, schemas, transactions)
from backend import persistable


class Capability(DDHbaseModel):
    ...


SchemaCapability = typing.ForwardRef('SchemaCapability')
Capabilities = typing.ForwardRef('Capabilities')


class SchemaCapability(Capability):
    """ Capability used for Schemas """
    Capabilities: typing.ClassVar[dict[str, SchemaCapability]] = {}
    supports_modes: typing.ClassVar[set[permissions.AccessMode]]  # supports_modes is a mandatory class variable
    by_modes: typing.ClassVar[dict[permissions.AccessMode, set[str]]] = {}

    @classmethod
    def __init_subclass__(cls):
        """ register subclass as .Capabilities """
        instance = cls()
        cls.Capabilities[cls.__name__] = instance
        # register instance in a set of mode supporters:
        [cls.by_modes.setdefault(m, set()).add(cls.__name__) for m in cls.supports_modes]
        return

    @classmethod
    def capabilities_for_modes(cls, modes: typing.Iterable[permissions.AccessMode]) -> set[Capabilities]:
        """ return the capabilities required for the access modes """
        caps = set.union(set(), *[c for m in modes if (c := cls.by_modes.get(m))])
        return {Capabilities(c) for c in caps}  # to Enum

    def apply(self, schema: schemas.AbstractSchema, access, transaction, data):
        return data


class Validate(SchemaCapability):
    supports_modes = set()


class Anonymize(SchemaCapability):
    supports_modes = {permissions.AccessMode.anonymous}

    def apply(self, schema, access, transaction, data_by_principal: dict):
        cache = {}
        return self.transform(schema, access, transaction, data_by_principal, cache)

    def transform(self, schema, access, transaction, data_by_principal: dict, cache: dict) -> dict:
        """ Apply self.transform_value to sensitivities in schema, keeping
            cache of mapped values, so same value always get's transformed into same
            value.
        """
        # selection is the path remaining after dispatching of the e_node:
        selection = str(access.ddhkey.without_variant_version().remainder(access.e_key_split))

        new_data_by_principal = {}  # new data, since keys (=principals) are different
        for principal_id, data in data_by_principal.items():  # data may have multiple principals
            # transform principal_id first:
            principal_id = self.transform_value(
                principal_id, '', '', schemas.Sensitivity.eid, access, transaction, cache)
            for sensitivity, path_fields in schema.schema_attributes.sensitivities.items():
                # transform all path and fields for a sensitivity
                data = schema.transform(path_fields, selection, data, self.transform_value,
                                        sensitivity, access, transaction, cache)

            new_data_by_principal[principal_id] = data

        return new_data_by_principal

    def transform_value(self, value, path, field, sensitivity, access, transaction, cache):
        """ transform a single value. Keep a cache per location and value.
        """
        if value in (None, ''):  # we don't need to transform non-existent or empty values
            v = None
        else:
            k = (path, field, value)
            v = cache.get(k)
            if v is None:

                match value:
                    case str():
                        v = secrets.token_urlsafe(max(10, len(value)))  # replace by random string of similar length
                    case int():
                        # add a random number in similar range (but at least 10000 to ensure randomness):
                        v = value + secrets.randbelow(max(10000, value))
                    case float():  # apply a multiplicative factor
                        factor = secrets.randbelow(10000)/5000  # 0..2
                        v = value*factor
                    case datetime.datetime():
                        v = datetime.datetime.now()
                    case datetime.date():
                        v = datetime.date.today()
                    case datetime.time():
                        v = datetime.datetime.now().time()
                    case _:  # anything else, just a random str:
                        v = secrets.token_urlsafe(10)
                cache[k] = v
        return v


class Pseudonymize(Anonymize):
    supports_modes = {permissions.AccessMode.pseudonym}

    def apply(self, schema, access, transaction, data_by_principal: dict):
        pm = PseudonymMap.create(access, transaction, data_by_principal)
        r = self.transform(schema, access, transaction, data_by_principal, pm.cache)
        transaction.add(persistable.PersistAction(obj=pm))
        return r


class PseudonymMap(persistable.Persistable):

    cache: dict

    @classmethod
    def create(cls, access, transaction: transactions.Transaction, data_by_principal: dict) -> typing.Self:
        cache = {('', '', pid): transaction.trxid+'/'+secrets.token_urlsafe(max(10, len(pid)))
                 for pid in data_by_principal.keys()}
        return PseudonymMap(cache=cache)


# Enum with all available Capabilities:
Capabilities = enum.Enum('Capabilities', [(n, n) for n in SchemaCapability.Capabilities], type=str, module=__name__)
