""" JSON Schema Format """
from __future__ import annotations
import typing
import pydantic
import json
import jsonschema
import jsonschema.validators


from core import schemas, keys, errors


class JsonSchemaElement(schemas.AbstractSchemaElement):

    definition: dict

    def to_schema(self) -> JsonSchema:
        return JsonSchema(json_schema=json.dumps(self.definition))


class JsonSchema(schemas.AbstractSchema):

    format_designator: typing.ClassVar[schemas.SchemaFormat] = schemas.SchemaFormat.json
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema=['application/openapi', 'application/json'], of_data=['application/json'])
    json_schema: pydantic.Json

    def __getitem__(self, key: keys.DDHkey, default=None, create_intermediate: bool = False) -> type[JsonSchemaElement] | None:
        return JsonSchemaElement(definition=self._descend_path(self.json_schema, key))

    def __iter__(self) -> typing.Iterator[tuple[keys.DDHkey, JsonSchemaElement]]:
        # TODO: Schema Iterator
        return iter([])

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> JsonSchema:
        return cls(json_schema=schema_str, schema_attributes=schema_attributes)

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        return self

    def to_output(self):
        """ return naked json schema """
        return self.json_schema

    @classmethod
    def _descend_path(cls, json_schema: pydantic.Json, path: keys.DDHkey):
        definitions = json_schema.get('definitions', {})
        current = json_schema  # before we descend path, this cls is at the current level
        pathit = iter(path)  # so we can peek whether we're at end
        for segment in pathit:
            segment = str(segment)
            # look up one segment of path, returning ModelField
            mf = current['properties'].get(str(segment), None)
            if mf is None:
                return None
            else:
                if (ref := mf.get('$ref', '')).startswith('#/definitions/'):
                    current = definitions.get(ref[len('#/definitions/'):])
                elif mf['type'] == 'array' and '$ref' in mf['items']:
                    if (ref := mf['items']['$ref']).startswith('#/definitions/'):
                        current = definitions.get(ref[len('#/definitions/'):])

                else:  # we're at a leaf, return
                    if next(pathit, None) is None:  # path ends here
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
        subs = self._descend_path(self.json_schema, remainder)
        if not subs:
            raise errors.ValidationError(f'Path {remainder} is not in schema')
        print(f'{self.__class__.__name__}.validate_data({type(data)}, {remainder=}, {no_extra=}, {subs=})')
        jsonschema.validate(instance=data['mgf'], schema=subs)
        return data

    def validate_schema(self):
        vcls = jsonschema.validators.validator_for(self.json_schema)
        vcls.check_schema(self.json_schema)
