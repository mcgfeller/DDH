""" Python internal Schema Format using Pydantic """
from __future__ import annotations
import typing
import pydantic

from core import schemas, keys, errors


class PySchemaElement(schemas.AbstractSchemaElement):
    """ A Pydantic AbstractSchema class """

    @classmethod
    def to_schema(cls) -> PySchema:
        return PySchema(schema_element=cls)

    @classmethod
    def iter_paths(cls, pk=()) -> typing.Generator[tuple[keys.DDHkey, type[PySchemaElement]], None, None]:
        """ recursive descent through schema yielding (key,schema_element) """
        yield (keys.DDHkey(pk), cls)  # yield ourselves first
        for k, mf in cls.__fields__.items():
            assert isinstance(mf, pydantic.fields.ModelField)
            sub_elem = mf.type_
            if issubclass(sub_elem, PySchemaElement):
                yield from sub_elem.iter_paths(pk+(k,))  # then descend
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
            mf = current.__fields__.get(str(segment), None)
            if mf is None:
                if create_intermediate:
                    new_current = cls.create_from_elements(segment)
                    current._add_fields(**{segment: (new_current, None)})
                    current = new_current
                else:
                    return None
            else:
                assert isinstance(mf, pydantic.fields.ModelField)
                assert mf.type_ is not None
                if issubclass(mf.type_, PySchemaElement):
                    current = mf.type_  # this is the next Pydantic class
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
    def extract_attributes(cls, path: keys.DDHkey, atts: schemas.SchemaAttributes):
        """ Extract attributes and insert them to schema.schema_attributes
        """
        # References:
        if issubclass(cls, PySchemaReference):
            atts.add_reference(path, cls)

        # Sensitivities - sensitivity entry in extra field:
        sensitivities = {fn: ex['sensitivity']
                         for fn, f in cls.__fields__.items() if 'sensitivity' in (ex := f.field_info.extra)}
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
        return pydantic.create_model(key, __base__=cls, **elements)


class PySchemaReference(schemas.AbstractSchemaReference, PySchemaElement):

    class Config:
        @staticmethod
        def schema_extra(schema: dict[str, typing.Any], model: typing.Type[PySchemaReference]) -> None:
            schema['properties']['dep'] = {'$ref': model.getURI()}
            return

    @classmethod
    def get_target(cls) -> keys.DDHkey:
        """ get target key - oh Pydantic! """
        return cls.__fields__['ddhkey'].default

    @classmethod
    def create_from_key(cls, ddhkey: keys.DDHkey, name: str | None = None) -> typing.Type[PySchemaReference]:
        name = name if name else str(ddhkey)
        m = PySchemaElement.create_from_elements(name, ddhkey=(keys.DDHkey, ddhkey))
        return typing.cast(typing.Type[PySchemaReference], m)


class PySchema(schemas.AbstractSchema):
    """ A AbstractSchema in Pydantic Python, containing a PySchemaElement """
    schema_element: typing.Type[PySchemaElement]
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema=['application/openapi', 'application/json'], of_data=['application/json'])

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

    def to_json_schema(self):
        """ Make a JSON Schema from this Schema """
        jcls = schemas.SchemaFormat2Class[schemas.SchemaFormat.json]
        return jcls(json_schema=self.schema_element.schema_json(), schema_attributes=self.schema_attributes)

    def to_output(self) -> pydantic.Json:
        """ Python schema is output as JSON """
        # return self.to_json_schema()
        return self.schema_element.schema_json()

    def _add_fields(self, fields: dict[str, tuple]):
        """ Add the field in dict to the schema element """
        self.schema_element._add_fields(**fields)
