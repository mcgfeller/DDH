""" JSON Schema Format """
from __future__ import annotations
import typing
import pydantic
import json


from core import schemas, keys, errors


class JsonSchemaElement(schemas.AbstractSchemaElement):

    @classmethod
    def to_schema(cls) -> JsonSchema:
        return JsonSchema(json_schema=cls)


class JsonSchema(schemas.AbstractSchema):

    format_designator: typing.ClassVar[schemas.SchemaFormat] = schemas.SchemaFormat.json
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema='application/openapi', of_data='application/json')
    json_schema: pydantic.Json

    def __getitem__(self, key: keys.DDHkey, default=None) -> type[JsonSchemaElement] | None:
        return self._descend_path(self.json_schema, key)

    def __setitem__(self, key: keys.DDHkey, value: type[JsonSchemaElement], create_intermediate: bool = True) -> type[JsonSchemaElement] | None:
        raise errors.SubClass

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> JsonSchema:
        return cls(json_schema=schema_str, schema_attributes=schema_attributes)

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        return self

    def to_output(self):
        """ return naked json schema """
        return self.json_schema

    def obtain(self, ddhkey: keys.DDHkey, split: int, create_intermediate: bool = False) -> schemas.AbstractSchema | None:
        """ obtain a schema for the ddhkey, which is split into the key holding the schema and
            the remaining path. 
        """
        khere, kremainder = ddhkey.split_at(split)
        if kremainder.key:
            s = None
            j_defn = self[kremainder]
            if j_defn:
                s = self.__class__.from_definition(j_defn)
            else: s = None  # not found
        else:
            s = self
        return s

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

    @classmethod
    def from_definition(cls, json_schema):
        # return cls(json_schema=json_schema)
        return cls(json_schema=json.dumps(json_schema))
