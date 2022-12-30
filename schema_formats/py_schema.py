""" Python internal Schema Format using Pydantic """
from __future__ import annotations
import typing
import pydantic

from utils.pydantic_utils import DDHbaseModel
from core import schemas, keys, versions, errors, principals, permissions, nodes, keydirectory
from frontend import user_auth


class PySchemaElement(schemas.AbstractSchemaElement):
    """ A Pydantic AbstractSchema class """

    @classmethod
    def to_schema(cls) -> PySchema:
        return PySchema(schema_element=cls)

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
                    new_current = pydantic.create_model(segment, __base__=PySchemaElement)
                    current.add_fields(**{segment: new_current})
                    current = new_current
                else:
                    return None
            else:
                assert isinstance(mf, pydantic.fields.ModelField)
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
        """ TODO: Extract attributes and insert them to schema.schema_attributes
        """
        return

    @classmethod
    def get_subschema_class(cls, subname) -> typing.Tuple:
        """ return subschema for this schema:
            class
            container
            id

            TODO: Only used in .get_resolver() - eliminate -> resolver should be an gatherable annotation.

        """
        sub = typing.get_type_hints(cls).get(str(subname))
        if sub is None:
            return (None, None, None)
        if isinstance(sub, PySchemaElement):
            return (sub, None, None)
        elif isinstance(sub, typing.GenericAlias) and sub.__origin__ is list and sub.__args__:
            innerclass = sub.__args__[0]
            princs = [n for n, t in innerclass.__fields__.items() if issubclass(t.type_,
                                                                                principals.Principal)]
            if princs:
                if 'id' in princs:
                    return (innerclass, sub.__origin__, 'id')
                else:
                    return (innerclass, sub.__origin__, princs[0])
            else:
                return (innerclass, sub.__origin__, None)
        else:
            raise errors.DAppError(f'Cannot understand element {subname}={sub} in {cls}')

    def get_resolver(self,  selection: keys.DDHkey, access: permissions.Access, q):
        # TODO: This is far too specific - redesign
        # ids : typing.Dict[type,typing.Dict[str,list]] = {} # {class : {idattr : [id,...]}}
        entire_selection = selection
        schema = self.__class__
        princs = user_auth.get_principals(access.ddhkey.owners)

        while len(selection.key):
            next_key, remainder = selection.split_at(1)  # next level
            schema, container, idattr = schema.get_subschema_class(next_key)
            if not schema:
                raise errors.NotFound(f'Invalid key {next_key} in {entire_selection}')
            if container:
                sel, remainder = remainder.split_at(1)  # next level is ids
            resolver = getattr(schema, 'resolve', None)
            if resolver:
                res = resolver(remainder, princs, q)
                return res
            selection = remainder
        else:  # there is no resolver so far, we cannot grab this without a further segment:
            raise errors.NotFound(f'Incomplete key: {entire_selection}')


class PySchemaReference(schemas.AbstractSchemaReference, PySchemaElement):

    class Config:
        @staticmethod
        def schema_extra(schema: dict[str, typing.Any], model: typing.Type[PySchemaReference]) -> None:
            schema['properties']['dep'] = {'$ref': model.getURI()}
            return

    @classmethod
    def getURI(cls) -> pydantic.AnyUrl:
        return typing.cast(pydantic.AnyUrl, str(cls.__fields__['ddhkey'].default))

    @classmethod
    def create_from_key(cls, ddhkey: keys.DDHkey, name: str | None = None) -> typing.Type[PySchemaReference]:
        name = name if name else str(ddhkey)
        m = pydantic.create_model(name, __base__=cls, ddhkey=(keys.DDHkey, ddhkey))
        return typing.cast(typing.Type[PySchemaReference], m)


class PySchema(schemas.AbstractSchema):
    """ A AbstractSchema in Pydantic Python, containing a PySchemaElement """
    schema_element: typing.Type[PySchemaElement]
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema='application/openapi', of_data='application/json')

    def __getitem__(self, key: keys.DDHkey, default=None, create_intermediate: bool = False) -> type[PySchemaElement] | None:
        se = self.schema_element.descend_path(key, create_intermediate=create_intermediate)
        return default if se is None else se

    def __setitem__(self, key: keys.DDHkey, value: type[PySchemaElement], create_intermediate: bool = True) -> type[PySchemaElement] | None:
        raise errors.SubClass

    def __iter__(self) -> typing.Iterator[tuple[keys.DDHkey, PySchemaElement]]:
        # TODO: Schema Iterator
        return iter([(keys.DDHkey(()), self.schema_element)])

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

    def add_fields(self, fields: dict[str, tuple]):
        """ Add the field in dict """
        self.schema_element.add_fields(**fields)

    def add_empty_schemas(self, names: list[str]) -> list[PySchema]:
        """ Add a sequence of empty models, returing them as a list """
        schemas = [PySchema(schema_element=pydantic.create_model(
            name, __base__=PySchemaElement)) for name in names]
        self.add_fields({name: (schema.schema_element, None)
                        for name, schema in zip(names, schemas)})
        return schemas
