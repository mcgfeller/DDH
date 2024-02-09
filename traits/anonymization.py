""" Executable Capabilities, especially for Schemas """
from __future__ import annotations

import abc
import enum
import typing
import secrets
import datetime

import pydantic
from utils.pydantic_utils import DDHbaseModel, CV, tuple_key_to_str, str_to_tuple_key

from core import (errors, keys, versions, permissions, schemas, transactions, trait, common_ids)
from backend import persistable
from . import capabilities


class Anonymize(capabilities.DataCapability):
    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.anonymous})
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.read})
    phase: CV[trait.Phase] = trait.Phase.post_load

    async def apply(self, traits: trait.Traits, trstate: trait.TransformerState, **kw: dict):
        assert trstate.parsed_data is not None and len(trstate.parsed_data) > 0
        cache = {}
        trstate.parsed_data = self.transform(trstate.nschema, trstate.access,
                                             trstate.transaction, trstate.parsed_data, cache)

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
                principal_id, '', '', str, schemas.Sensitivity.eid, access, transaction, cache)
            for sensitivity, path_fields in schema.schema_attributes.sensitivities.items():
                # transform all path and fields for a sensitivity
                data = schema.transform(path_fields, selection, data, self.transform_value,
                                        sensitivity, access, transaction, cache)

            new_data_by_principal[principal_id] = data

        return new_data_by_principal

    def transform_value(self, value, path, field, typ: type, sensitivity, access, transaction, cache):
        """ transform a single value. Keep a cache per location and value.
        """
        if value in (None, ''):  # we don't need to transform non-existent or empty values
            v = None
        else:
            k = (path, field, value)
            v = cache.get(k)
            if v is None:
                # unfortunately, we cannot use match with provided typ, as is doesn't check for subclass
                if issubclass(typ, str):
                    v = secrets.token_urlsafe(max(10, len(value)))  # replace by random string of similar length
                elif issubclass(typ, int):
                    # add a random number in similar range (but at least 10000 to ensure randomness):
                    v = value + secrets.randbelow(max(10000, value))
                elif issubclass(typ, float):  # apply a multiplicative factor
                    factor = secrets.randbelow(10000)/5000  # 0..2
                    v = value*factor
                elif issubclass(typ, datetime.datetime):
                    v = datetime.datetime.now()
                elif issubclass(typ, datetime.date):
                    v = datetime.date.today()
                elif issubclass(typ, datetime.time):
                    v = datetime.datetime.now().time()
                else:  # anything else, just a random str:
                    v = secrets.token_urlsafe(10)
                cache[k] = v
        return v


class Pseudonymize(Anonymize):
    supports_modes: CV[frozenset[permissions.AccessMode]] = {permissions.AccessMode.pseudonym}

    async def apply(self, traits: trait.Traits, trstate: trait.TransformerState, **kw: dict):
        assert trstate.parsed_data is not None and len(trstate.parsed_data) > 0
        cache = {}
        for pid in trstate.parsed_data.keys():
            tid = trstate.transaction.trxid+'_'+secrets.token_urlsafe(max(10, len(pid)))
            cache[('', '', pid)] = tid

        trstate.parsed_data = self.transform(trstate.nschema, trstate.access,
                                             trstate.transaction,  trstate.parsed_data, cache)
        # the cache was filled during the transform - save it per principal:
        for tid, data in trstate.parsed_data.items():
            pm = PseudonymMap(id=typing.cast(common_ids.PersistId, tid), cache=cache)
            pm.invert()
            trstate.transaction.add(persistable.SystemDataPersistAction(obj=pm))
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
        e = self.model_copy()
        e.cache.clear()  # original cache is not exported
        e.inverted_cache = {tuple_key_to_str(k): v for k, v in e.inverted_cache.items()}
        return e.model_dump_json()

    @classmethod
    def from_json(cls, j: str) -> typing.Self:
        """ Convert back dict keys encoded in .to_json() """
        o = cls.model_validate_json(j)
        assert o.inverted_cache is not None
        o.inverted_cache = {str_to_tuple_key(k): v for k, v in o.inverted_cache.items()}
        return o


class DePseudonymize(capabilities.DataCapability):
    """ Revert the pseudonymization based on the stored map """

    supports_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.pseudonym})
    only_modes: CV[frozenset[permissions.AccessMode]] = frozenset({permissions.AccessMode.write})
    phase: CV[trait.Phase] = trait.Phase.pre_store  # after validation
    after: str = 'ValidateToDApp'  # we don't reveil identity to DApp

    async def apply(self, traits: trait.Traits, trstate: trait.TransformerState, **kw: dict):
        eid = trstate.access.original_ddhkey.owner  # this is the pseudo-owner uder which the map is stored
        try:
            pm = await PseudonymMap.load(eid, trstate.access.principal, trstate.transaction)  # retrieve it
        except KeyError:
            raise errors.NotFound(f'Not a valid pseudonymous id: {eid}').to_http()
        assert trstate.parsed_data is not None and len(trstate.parsed_data) > 0
        assert pm.inverted_cache
        trstate.parsed_data = self.transform(trstate.nschema, trstate.access,
                                             trstate.transaction, trstate.parsed_data, eid, pm.inverted_cache)
        return

    def transform(self, schema, access, transaction, data: dict, eid, lookup: dict) -> dict:
        """ Apply self.transform_value to sensitivities in schema, read from the inverse 
            cache so orinal values are restored. 
        """
        # selection is the path remaining after dispatching of the e_node:
        selection = str(access.ddhkey.without_variant_version().remainder(access.schema_key_split))

        # transform principal_id first:
        principal_id = self.transform_value(
            eid, '', '', str, schemas.Sensitivity.eid, access, transaction, lookup)
        for sensitivity, path_fields in schema.schema_attributes.sensitivities.items():
            # transform all path and fields for a sensitivity
            data = schema.transform(path_fields, selection, data, self.transform_value,
                                    sensitivity, access, transaction, lookup)
        access.ddhkey = access.ddhkey.with_new_owner(principal_id)
        return data

    def transform_value(self, value, path, field, typ: type, sensitivity, access, transaction, lookup):
        """ inverse transform a single value.
        """
        if value in (None, ''):  # we don't need to transform non-existent or empty values
            v = None
        else:
            k = (path, field, value)
            v = lookup.get(k, value)
        return v


async def resolve_owner(access, transaction):
    """ modify access.ddhkey according to real owner.
        access.original_ddhkey stays:
    """
    eid = access.ddhkey.owner
    try:
        # retrieve it
        pm = await PseudonymMap.load(eid, access.principal, transaction)
        owner = pm.inverted_cache[('', '', eid)]
    except KeyError:
        raise errors.NotFound(f'Not a valid pseudonymous id: {eid}').to_http()

    access.ddhkey = access.ddhkey.with_new_owner(owner)
    return
