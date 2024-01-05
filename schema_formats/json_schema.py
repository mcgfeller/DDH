""" JSON Schema Format """
from __future__ import annotations
import typing
import pydantic
import json
import functools
import jsonschema
import jsonschema.validators
import jsonschema.exceptions

from utils.pydantic_utils import CV

# to overwrite jsonschema datetime format checker:
import jsonschema._format
import datetime


from core import schemas, keys, errors


class JsonSchemaElement(schemas.AbstractSchemaElement):

    definition: dict

    def to_schema(self) -> JsonSchema:
        return JsonSchema(json_schema=json.dumps(self.definition))


class JsonSchemaReference(schemas.AbstractSchemaReference, JsonSchemaElement):

    # FIXME[pydantic] #32: We couldn't refactor this class, please create the `model_config` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.

    @staticmethod
    def _json_schema_extra(schema: dict[str, typing.Any], model: typing.Type[JsonSchemaReference]) -> None:
        schema['properties']['dep'] = {self.d_ref: model.getURI()}
        return

    model_config = pydantic.ConfigDict(json_schema_extra=_json_schema_extra)

    @classmethod
    def get_target(cls) -> keys.DDHkey:
        """ get target key - oh Pydantic! """
        return cls.model_fields['ddhkey'].default

    @classmethod
    def create_from_key(cls, ddhkey: keys.DDHkeyRange, name: str | None = None) -> typing.Type[JsonSchemaReference]:
        name = name if name else str(ddhkey)
        m = cls(definition={JsonSchema.d_ref: str(ddhkey)}).__class__
        return m


_Json2Python: dict[tuple[str, str | None], type] = {
    ('string', 'date-time'): datetime.datetime,
    ('string', 'date'): datetime.date,
    ('string', 'time'): datetime.time,
    ('string', 'duration'): datetime.timedelta,
}

_empty_marker = object()


class JsonSchema(schemas.AbstractSchema):

    format_designator: CV[schemas.SchemaFormat] = schemas.SchemaFormat.json
    mimetypes: CV[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema=['application/openapi', 'application/json'], of_data=['application/json'])
    json_schema: pydantic.Json
    _v_validators: dict[keys.DDHkey, json_schema.validators.Validator] = {}  # Cache
    _v_descend_cache: dict[tuple[keys.DDHkey | tuple, bool], dict | None] = {}

    d_ref: CV[str] = '$ref'
    d_defs1: CV[str] = '$defs'
    d_defs: CV[str] = '#/$defs/'

    def __getitem__(self, key: keys.DDHkey, default=None, create_intermediate: bool = False) -> type[JsonSchemaElement] | None:
        d = self.descend_path(key, create_intermediate=create_intermediate)
        if d is None:
            assert not create_intermediate
            return default
        else:
            return JsonSchemaElement(definition=d)

    def __iter__(self) -> typing.Iterator[tuple[keys.DDHkey, JsonSchemaElement]]:
        # TODO: Schema Iterator
        return iter([])

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> JsonSchema:
        return cls(json_schema=schema_str, schema_attributes=schema_attributes)

    @classmethod
    def get_reference_class(cls) -> type[JsonSchemaReference]:
        """ get class of concrete AbstractSchemaReference associated with this concrete Schema """
        return JsonSchemaReference

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        return self

    def to_output(self):
        """ return naked json schema """
        return self.json_schema

    def descend_path(self, path: keys.DDHkey, create_intermediate: bool = False):
        """ descend path with local cache """
        v = self._v_descend_cache.get((path, create_intermediate), _empty_marker)
        if v is _empty_marker:
            v = self._descend_path(path, create_intermediate=create_intermediate)
            self._v_descend_cache[(path, create_intermediate)] = v
        return v

    def _descend_path(self, path: keys.DDHkey, create_intermediate: bool = False):
        definitions = self.json_schema.get(self.d_defs1, {})
        current = self.json_schema  # before we descend path, this cls is at the current level
        pathit = iter(path)  # so we can peek whether we're at end
        for segment in pathit:
            segment = str(segment)
            # look up one segment of path, returning ModelField
            assert 'properties' in current
            fi = current['properties'].get(str(segment), None)
            if fi is None:
                if create_intermediate:
                    new_current = self.create_from_elements(segment)
                    current['properties'][segment] = new_current
                    current = new_current
                else:
                    return None
            else:
                # #32: New Pyd2 optional type json: anyof, first element is value, 2nd is null:
                if (fa := fi.get('anyOf')) and len(fa) == 2 and fa[1].get('type') == 'null':
                    fi = fa[0]

                if (ref := fi.get(self.d_ref, '')).startswith(self.d_defs):
                    current = definitions.get(ref[len(self.d_defs):])
                elif fi.get('type') == 'array' and self.d_ref in fi['items']:
                    if (ref := fi['items'][self.d_ref]).startswith(self.d_defs):
                        current = definitions.get(ref[len(self.d_defs):])

                else:  # we're at a leaf, return
                    if next(pathit, None) is None:  # path ends here
                        current = fi
                        break
                    else:  # path continues beyond this point, so this is not found and not creatable
                        return None
        return current

    def parse(self, data: bytes) -> dict:
        if isinstance(data, dict):
            d = data
        else:
            d = json.loads(data)  # make dict
        return d

    def validate_data(self, data: dict, remainder: keys.DDHkey, no_extra: bool = True) -> dict:
        """ Validate data at subschema path remainder. 
            data is already parsed as dict. 
        """

        print(f'{self.__class__.__name__}.validate_data({type(data)}, {remainder=}, {no_extra=})')
        validator = self._v_validators.get(remainder)  # cached?
        if not validator:
            subs = self.descend_path(remainder)
            if not subs:
                raise errors.NotFound(f'Path {remainder} is not in schema')
            vcls = jsonschema.validators.validator_for(subs)  # find correct validator for schema.
            validator = vcls(subs, format_checker=vcls.FORMAT_CHECKER)  # instantiate for subschema
            self._v_validators[remainder] = validator  # cache it
        error = jsonschema.exceptions.best_match(validator.iter_errors(data))
        if error is not None:
            raise error

        return data

    def validate_schema(self, no_extra: bool = True):
        """ validate and cache root schema """
        vcls = jsonschema.validators.validator_for(self.json_schema)
        vcls.check_schema(self.json_schema)
        validator = vcls(self.json_schema, format_checker=vcls.FORMAT_CHECKER)
        self._v_validators[keys.DDHkey(())] = validator  # cache it

    @classmethod
    def create_from_elements(cls, key: keys.DDHkey | tuple | str, **elements: typing.Mapping[str, tuple[type, typing.Any]]) -> dict:
        """ Create a named SchemaElement from a Mapping of elements, which {name : (type,default)} """
        # if isinstance(key, keys.DDHkey):
        #     key = key.key
        # if isinstance(key, tuple):
        #     key = '_'.join(key)

        p = {n: {'type': t} for n, (t, d) in elements.items()}
        r = [n for n, (t, d) in elements.items() if d is None]

        o = {
            "type": "object",
            "required": r,
            "properties": p,
        }
        return o

    def get_type(self, path, field, value) -> type:
        """ return the Python type of a path, field """
        pt = None
        p = self.descend_path((path, field))
        if p:
            jt = p['type']; jf = p.get('format')
            pt = _Json2Python.get((jt, jf))
        return pt if pt else type(value)


# #32: Unfortunately, pydantic.datetime_parse.parse_datetime disappeared in Pyd2:
_parse_datetime = pydantic.TypeAdapter(datetime.datetime).validate_strings


@jsonschema._format._checks_drafts(name="date-time")
def is_datetime(instance: object) -> bool:
    """ json_schema DateTime format check is more restrictive than and not compatible
        with Pydantic; i.e., it requires a timezone designator for datetimes.
        Overwrite the date-time format check using Pydantic's datetime_parse.
    """
    try:
        d = _parse_datetime(instance)  # type:ignore
        return True
    except ValueError:
        return False
