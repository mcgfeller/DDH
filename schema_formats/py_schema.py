""" Python internal Schema Format using Pydantic """
from __future__ import annotations
import typing
import types
import pydantic
from utils import pydantic_utils
import json


from core import schemas, keys, errors


class PySchemaElement(schemas.AbstractSchemaElement):
    """ A Pydantic AbstractSchema class """

    @classmethod
    def to_schema(cls) -> PySchema:
        return PySchema(schema_element=cls)

    @classmethod
    def iter_paths(cls, pk=()) -> typing.Generator[tuple[keys.DDHkey, type[PySchemaElement]], None, None]:
        """ recursive descent through schema yielding (key,schema_element) """
        # print(f'iter_paths {cls.__name__} {pk=}')
        yield (keys.DDHkey(pk), cls)  # yield ourselves first
        for k, fi in cls.model_fields.items():
            assert isinstance(fi, pydantic.fields.FieldInfo)
            sub_elem = pydantic_utils.type_from_fi(fi)
            if issubclass(sub_elem, PySchemaElement):

                yield from sub_elem.iter_paths(pk+((k,) if k else ()))  # then descend
        return

    @classmethod
    def descend_path(cls, path: keys.DDHkey, create_intermediate: bool = False) -> typing.Type[PySchemaElement] | None:
        """ Travel down PySchemaElement along path using some Pydantic implementation details.
            If a path segment is not found, return None, unless create is specified.
            Create inserts an empty schemaElement and descends further. 
            If a path ends with a simple datatype, we return its parent.  

        """
        current = cls  # before we descend path, this cls is at the current level
        pathit = iter(path)  # so we can peek whether we're at end
        for segment in pathit:
            segment = str(segment)
            # look up one segment of path, returning ModelField
            fi = current.model_fields.get(str(segment), None)
            if fi is None:
                if create_intermediate:
                    new_current = current.create_from_elements(segment)
                    current._add_fields(**{segment: (new_current, None)})
                    current = new_current
                    # print(f'create_intermediate, {cls=}, {path=}, {segment=}')
                    list(current.iter_paths())  # DEBUG TEST
                else:
                    return None
            else:
                assert isinstance(fi, pydantic.fields.FieldInfo)
                assert fi.annotation is not None
                sub_elem = pydantic_utils.type_from_fi(fi)
                if issubclass(sub_elem, PySchemaElement):
                    current = sub_elem  # this is the next Pydantic class
                else:  # we're at a leaf, return
                    if next(pathit, None) is None:  # path ends here
                        break
                    else:  # path continues beyond this point, so this is not found and not creatable
                        if create_intermediate:
                            raise ValueError(
                                f'Cannot create {segment=} of {path=} because it {current} is a simple datatype.')
                        else:
                            return None
        return current

    @classmethod
    def resolve(cls, remainder, principal, q) -> dict:
        """ resolve on all subschemas, returning data.
            If schema provides data at its level, refine .resolve() and
            call super().resolve()
        """
        d = {}
        for k, fi in cls.model_fields.items():
            assert isinstance(fi, pydantic.fields.FieldInfo)
            sub_elem = pydantic_utils.type_from_fi(fi)
            if issubclass(sub_elem, PySchemaElement):
                d[k] = sub_elem.resolve(remainder[:-1], principal, q)  # then descend
        return d

    @classmethod
    def extract_attributes(cls, path: keys.DDHkey, atts: schemas.SchemaAttributes):
        """ Extract attributes and insert them to schema.schema_attributes
        """
        # References:
        if issubclass(cls, PySchemaReference):
            atts.add_reference(path, cls)

        # Sensitivities - sensitivity entry in extra field:
        sensitivities = {fn: ex['sensitivity']
                         for fn, f in cls.model_fields.items() if (ex := f.json_schema_extra) and 'sensitivity' in ex}
        if sensitivities:
            atts.add_sensitivities(path, sensitivities)
        return

    @classmethod
    def create_from_elements(cls, key: keys.DDHkey | tuple | str, **elements: typing.Mapping[str, tuple[type, typing.Any]]) -> typing.Self:
        """ Create a named SchemaElement from a Mapping of elements, which {name : (type,default)} """
        if isinstance(key, keys.DDHkey):
            key = key.key
        if isinstance(key, tuple):
            key = '_'.join(key)
        elif isinstance(key, str):
            key = key.replace('/', '_')
        # se = pydantic.create_model(key, __base__=PySchemaElement, **elements)
        se = pydantic.create_model(key, __base__=cls, **elements)
        return se


class PySchemaReference(schemas.AbstractSchemaReference, PySchemaElement):

    @staticmethod
    def _json_schema_extra(schema: dict[str, typing.Any], model: typing.Type[PySchemaReference]) -> None:
        """ Generate  JSON Schema as a reference to the URI.

            NOTE #43: As Pydantic 2 cannot include an external $ref, we name them $xref and
            replace the text in PySchema.to_output().
        """
        schema['properties']['dep'] = {'$xref': model.getURI()}
        return
    model_config = pydantic.ConfigDict(json_schema_extra=_json_schema_extra)

    @classmethod
    def get_target(cls) -> keys.DDHkey:
        """ get target key - oh Pydantic! """
        return cls.model_fields['ddhkey'].default

    @classmethod
    def create_from_key(cls, ddhkey: keys.DDHkeyRange, name: str | None = None) -> typing.Type[PySchemaReference]:
        name = name if name else str(ddhkey)
        m = cls.create_from_elements(name, ddhkey=(keys.DDHkeyRange, ddhkey))
        return m  # typing.cast(typing.Type[PySchemaReference], m)


class PySchema(schemas.AbstractSchema):
    """ A AbstractSchema in Pydantic Python, containing a PySchemaElement """
    format_designator: typing.ClassVar[schemas.SchemaFormat] = schemas.SchemaFormat.internal
    schema_element: typing.Type[PySchemaElement]
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema=['application/openapi', 'application/json'], of_data=['application/json'])

    @pydantic.field_validator('schema_element', mode='after')
    @classmethod
    def not_root_class(cls, value) -> typing.Type[PySchemaElement]:
        if value is PySchemaElement:
            raise ValueError('schema_element must be a proper subclass of PySchemaElement')
        return value

    def __getitem__(self, key: keys.DDHkey, default=None, create_intermediate: bool = False) -> type[PySchemaElement] | None:
        se = self.schema_element.descend_path(key, create_intermediate=create_intermediate)
        return default if se is None else se

    def __iter__(self) -> typing.Iterator[tuple[keys.DDHkey, type[PySchemaElement]]]:
        """ Schema Iterator: yields (key,SchemaElement) pairs, ignoring primitive types.
        """
        return self.schema_element.iter_paths()

    @classmethod
    def get_reference_class(cls) -> type[PySchemaReference]:
        """ get class of concrete AbstractSchemaReference associated with this concrete Schema """
        return PySchemaReference

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> PySchema:
        raise NotImplementedError('PySchema cannot be created from string')

    @classmethod
    def create_empty(cls, ddhkey: keys.DDHkeyVersioned0 | str = 'dummy') -> typing.Self:
        """ create empty schema """
        return cls(schema_element=PySchemaElement.create_from_elements(key=ddhkey))

    def to_json_schema(self):
        """ Make a JSON Schema from this Schema """
        jcls = schemas.SchemaFormat2Class[schemas.SchemaFormat.json]
        js = jcls(json_schema=self.to_output(), schema_attributes=self.schema_attributes.model_copy())
        js._w_container = self._w_container  # copy container ref
        return js

    def to_output(self) -> pydantic.Json:
        """ Python schema is output as JSON """
        j = json.dumps(self.schema_element.model_json_schema())
        # NOTE #43: As Pydantic 2 cannot include an external $ref, we name them $xref and
        # replace the text here.
        j = j.replace('"$xref"', '"$ref"')
        return j

    def _add_fields(self, fields: dict[str, tuple]):
        """ Add the field in dict to the schema element """
        self.schema_element._add_fields(**fields)

    def parse(self, data: bytes) -> dict:
        if isinstance(data, dict):
            d = data
        else:
            d = json.loads(data)  # make dict
        return d

    def validate_data(self, data: dict, remainder: keys.DDHkey, no_extra: bool = True) -> dict:
        subs = self.schema_element.descend_path(remainder)
        print(f'{self.__class__.__name__}.validate_data({type(data)}, {remainder=}, {no_extra=}, {subs=})')
        if subs:
            data = subs.model_validate(data)
        else:
            raise errors.NotFound(f'Path {remainder} is not in schema')

        return data

    def get_type(self, path, field, value) -> type:
        """ return the Python type of a path, field """
        return type(value)


def SchemaField(*a, sensitivity: schemas.Sensitivity | None = None, **kw):
    """ Helper to build a field with sensitivity """
    e = kw.setdefault('json_schema_extra', {})
    e['sensitivity'] = sensitivity
    return pydantic.Field(*a, **kw)
