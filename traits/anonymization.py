""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, tuple_key_to_str, str_to_tuple_key

from core import (errors, versions, permissions, schemas, transactions, trait, common_ids)
from backend import persistable
from . import capabilities


class Anonymize(capabilities.DataCapability):
    supports_modes = {permissions.AccessMode.anonymous}
    only_modes = {permissions.AccessMode.read}
    phase = trait.Phase.post_load

    async def apply(self, traits: trait.Traits, trargs: trait.TransformerArgs, **kw: dict):
        assert trargs.parsed_data is not None and len(trargs.parsed_data) > 0
        cache = {}
        trargs.parsed_data = self.transform(trargs.nschema, trargs.access,
                                            trargs.transaction, trargs.parsed_data, cache)

    def transform(self, schema, access, transaction, data_by_principal: dict, cache: dict) -> dict:
        """ Apply self.transform_value to sensitivities in schema, keeping
            cache of mapped values, so same value always get's transformed into same
            value.
        """
        # selection is the path remaining after dispatching of the e_node:
        selection = str(access.ddhkey.without_variant_version().remainder(access.schema_key_split))

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

    async def apply(self, traits: trait.Traits, trargs: trait.TransformerArgs, **kw: dict):
        assert trargs.parsed_data is not None and len(trargs.parsed_data) > 0
        cache = {}
        for pid in trargs.parsed_data.keys():
            tid = trargs.transaction.trxid+'/'+secrets.token_urlsafe(max(10, len(pid)))
            cache[('', '', pid)] = tid

        trargs.parsed_data = self.transform(trargs.nschema, trargs.access,
                                            trargs.transaction,  trargs.parsed_data, cache)
        # the cache was filled during the transform - save it per principal:
        for tid, data in trargs.parsed_data.items():
            pm = PseudonymMap(id=typing.cast(common_ids.PersistId, tid), cache=cache)
            pm.invert()
            trargs.transaction.add(persistable.SystemDataPersistAction(obj=pm))
        return


class PseudonymMap(persistable.Persistable):

    cache: dict
    inverted_cache: dict | None = None  # for writing

    def invert(self):
        """ invert the cache, so it can be read """
        self.inverted_cache = {(path, field, anon): value for (path, field, value), anon in self.cache.items()}
        return

    def to_json(self) -> str:
        """ JSON export doesn't support dicts with tuple keyes. So convert them to str and convert back in .from_json() """
        if self.inverted_cache is None:
            self.invert()
        e = self.copy()
        e.cache.clear()  # original cache is not exported
        e.inverted_cache = {tuple_key_to_str(k): v for k, v in e.inverted_cache.items()}
        return e.json()

    @classmethod
    def from_json(cls, j: str) -> typing.Self:
        """ Convert back dict keys encoded in .to_json() """
        o = cls.parse_raw(j)
        assert o.inverted_cache is not None
        o.inverted_cache = {str_to_tuple_key(k): v for k, v in o.inverted_cache.items()}
        return o


class DePseudonymize(capabilities.DataCapability):
    supports_modes = {permissions.AccessMode.pseudonym}
    only_modes = {permissions.AccessMode.write}
    phase = trait.Phase.pre_store  # before validation!

    async def apply(self, traits: trait.Traits, trargs: trait.TransformerArgs, **kw: dict):
        eid = trargs.access.ddhkey.owner
        try:
            pm = await PseudonymMap.load(eid, trargs.access.principal, trargs.transaction)  # retrieve it
        except KeyError:
            raise errors.NotFound(f'Not a valid pseudonymous id: {eid}').to_http()
        assert trargs.parsed_data is not None and len(trargs.parsed_data) > 0
        return
