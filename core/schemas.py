""" DDH Core AbstractSchema Models """
from __future__ import annotations


import enum
import typing
import  abc

import pydantic

from utils.pydantic_utils import NoCopyBaseModel
from . import keys,permissions,errors,principals,versions
from . import keydirectory, nodes
from frontend import user_auth

@enum.unique
class Sensitivity(str,enum.Enum):
    """ Sensitivity, according to Fung et al., of use in export restrictions and anonymization.
    """

    ei = 'explicit id'
    qi = 'quasi id'
    sa = 'sensitive attribute'
    nsa = 'non-sensitive attribute'


#    def __repr__(self): return self.value

class SchemaElement(NoCopyBaseModel): 
    """ A Pydantic AbstractSchema class """

    @classmethod 
    def descend_path(cls,path: keys.DDHkey,create: bool =False) -> typing.Optional[typing.Type[SchemaElement]]:
        """ Travel down SchemaElement along path using some Pydantic implementation details.
            If a path segment is not found, return None, unless create is specified.
            Create inserts an empty schemaElement and descends further. 
            If a path ends with a simple datatype, we return its parent.  

        """ 
        current = cls # before we descend path, this cls is at the current level 
        pathit = iter(path) # so we can peek whether we're at end
        for segment in pathit:
            segment = str(segment)
            mf = current.__fields__.get(str(segment),None) # look up one segment of path, returning ModelField
            if mf is None:
                if create: 
                    new_current = pydantic.create_model(segment, __base__=SchemaElement)
                    current.add_fields(**{segment:new_current})
                    current = new_current
                else:
                    return None
            else: 
                assert isinstance(mf,pydantic.fields.ModelField)
                if issubclass(mf.type_,SchemaElement):
                    current = mf.type_ # this is the next Pydantic class
                else: # we're at a leaf, return
                    if next(pathit,None) is None: # path ends here
                        break 
                    else: # path continues beyond this point, so this is not found and not creatable
                        if create:
                            raise ValueError(f'Cannot create {segment=} of {path=} because it {current} is a simple datatype.')
                        else:
                            return None 
        return current


    @classmethod
    def get_subschema_class(cls, subname) -> typing.Tuple:
        """ return subschema for this schema:
            class
            container
            id

        """
        sub = typing.get_type_hints(cls).get(str(subname))
        if sub is None:
            return (None,None,None)
        if isinstance(sub,SchemaElement):
            return (sub,None,None)
        elif isinstance(sub,typing.GenericAlias) and sub.__origin__ is list and sub.__args__:
            innerclass = sub.__args__[0]
            princs = [n for n,t in innerclass.__fields__.items() if issubclass(t.type_,principals.Principal)]
            if princs:
                if 'id' in princs:
                    return (innerclass,sub.__origin__,'id')
                else:
                    return (innerclass,sub.__origin__,princs[0])
            else:
                return (innerclass,sub.__origin__,None)
        else:
            raise errors.DAppError(f'Cannot understand element {subname}={sub} in {cls}')

    def get_resolver(self,  selection: keys.DDHkey,access: permissions.Access, q):
        # ids : typing.Dict[type,typing.Dict[str,list]] = {} # {class : {idattr : [id,...]}}
        entire_selection = selection
        schema = self.__class__
        princs = user_auth.get_principals(access.ddhkey.owners)

        while len(selection.key):
            next_key,remainder = selection.split_at(1) # next level
            schema,container,idattr = schema.get_subschema_class(next_key)
            if not schema:
                raise errors.NotFound(f'Invalid key {next_key} in {entire_selection}') 
            if container:
                sel,remainder = remainder.split_at(1) # next level is ids
            resolver = getattr(schema,'resolve',None)
            if resolver:
                res = resolver(remainder,princs, q)
                return res
            selection = remainder
        else: # there is no resolver so far, we cannot grab this without a further segment:
            raise errors.NotFound(f'Incomplete key: {entire_selection}')

    @classmethod
    def replace_by_schema(cls,ddhkey : keys.DDHkey, schema_attributes : typing.Optional[SchemaAttributes]) -> type[SchemaReference]:
        """ Replace this SchemaElement by a proper schema with attributes, 
            and return the SchemaReference to it, which can be used like a SchemaElement.
        """
        s = PySchema(schema_attributes=schema_attributes or SchemaAttributes(),schema_element=cls)
        snode = nodes.SchemaNode(owner=principals.RootPrincipal,schema=s,consents=AbstractSchema.get_schema_consents())
        keydirectory.NodeRegistry[ddhkey] = snode
        schemaref = SchemaReference.create_from_key(str(ddhkey),ddhkey=ddhkey)
        return schemaref


class SchemaReference(SchemaElement):

    ddhkey : typing.ClassVar[str] 
    version_required : versions.VersionConstraint = pydantic.Field(default=versions.NoConstraint,description="Constrains the version of the target schema")

    class Config:
        @staticmethod
        def schema_extra(schema: dict[str, typing.Any], model: typing.Type[SchemaReference]) -> None:
            schema['properties']['dep'] =  {'$ref': model.getURI()}
            return

    @classmethod
    def getURI(cls) -> pydantic.AnyUrl:
        return typing.cast(pydantic.AnyUrl,str(cls.__fields__['ddhkey'].default))

    @classmethod
    def create_from_key(cls,name: str, ddhkey : keys.DDHkey) -> typing.Type[SchemaReference]:
        m = pydantic.create_model(name,__base__ = cls,ddhkey = (keys.DDHkey,ddhkey))
        return typing.cast(typing.Type[SchemaReference],m)



@enum.unique
class Requires(str,enum.Enum):
    """ Schema Data requirements """

    one = 'one'
    few = 'few'
    specific = 'specific'
    many = 'many'

@enum.unique
class SchemaFormat(str,enum.Enum): 
    internal = 'internal'
    json = 'json'
    xsd = 'xsd'


class SchemaAttributes(NoCopyBaseModel):
    format : typing.Optional[SchemaFormat] = pydantic.Field(SchemaFormat.json,description="The schema format for this instance")
    version : typing.Optional[versions.Version] = pydantic.Field(versions.Unspecified,description="The version of this schema instance")
    requires : typing.Optional[Requires] = None



class AbstractSchema(NoCopyBaseModel,abc.ABC):
    schema_attributes : SchemaAttributes = pydantic.Field(default=SchemaAttributes(),descriptor="Attributes associated with this Schema")

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        raise NotImplemented('conversion to JSON schema not supported')

    @classmethod
    @abc.abstractmethod 
    def from_str(cls,schema_str: str, schema_attributes : SchemaAttributes) -> AbstractSchema:
        ...


    def obtain(self,ddhkey: keys.DDHkey,split: int,create : bool = False) -> typing.Optional[AbstractSchema]:
        return None

    def format(self,format : SchemaFormat):
        if format == self.format:
            return self.to_output()
        if SchemaFormat2Class[format.value] == JsonSchema:
            return self.to_json_schema().to_output()
        else:
            raise NotImplementedError(f'output format {format} not supported for {self.__class__.__name__}')

    @abc.abstractmethod 
    def to_output(self) ->str:
        """ native output represenation """
        ...

    def add_fields(self,fields : dict):
        raise NotImplementedError('Field adding not supported in this schema')

    @staticmethod
    def insert_schema(id, schemakey: keys.DDHkey,transaction):
        # get a parent scheme to hook into
        upnode,split = keydirectory.NodeRegistry.get_node(schemakey,nodes.NodeSupports.schema,transaction)
        pkey = schemakey.up()
        if not pkey:
            raise ValueError(f'{schemakey} key is too high {self!r}')
        upnode = typing.cast(nodes.SchemaNode,upnode)
        # TODO: We should check some ownership permission here!
        parent = upnode.get_sub_schema(pkey,split,create=True) # create missing segments
        assert parent # must exist because create=True

        # now insert our schema into the parent's:
        schemaref = SchemaReference.create_from_key(id,ddhkey=schemakey)
        parent.add_fields({schemakey[-1] : (schemaref,None)})
        return schemaref

    @staticmethod
    def get_schema_consents() -> permissions.Consents:
        """ Schema world read access consents """
        return permissions.Consents(consents=[permissions.Consent(grantedTo=[principals.AllPrincipal],withModes={permissions.AccessMode.schema_read})])



class PySchema(AbstractSchema):
    """ A AbstractSchema in Pydantic Python, containing a SchemaElement """ 
    schema_element : typing.Type[SchemaElement]
    
    @classmethod
    def from_str(cls,schema_str: str, schema_attributes : SchemaAttributes) -> PySchema:
        raise NotImplementedError('PySchema cannot be created from string')


    def obtain(self,ddhkey: keys.DDHkey,split: int,create : bool = False) -> typing.Optional[AbstractSchema]:
        """ obtain a schema for the ddhkey, which is split into the key holding the schema and
            the remaining path. 
        """
        khere,kremainder = ddhkey.split_at(split)
        if kremainder:
            schema_element = self.schema_element.descend_path(kremainder,create=create)
            if schema_element:
                s = PySchema(schema_element=schema_element)
            else: s = None # not found
        else:
            s = self
        return s

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        return JsonSchema(json_schema=self.schema_element.schema_json(),schema_attributes=self.schema_attributes)


    def to_output(self):
        """ Python schema is output as JSON """
        return  self.schema_element.schema_json()

    def add_fields(self,fields : dict[str,tuple]):
        """ Add the field in dict """
        self.schema_element.add_fields(**fields)
    
    def add_empty_schemas(self,names : list[str]) -> list[PySchema]:
        """ Add a sequence of empty models, returing them as a list """
        schemas = [PySchema(schema_element=pydantic.create_model(name, __base__=SchemaElement)) for name in names]
        self.add_fields({name : (schema.schema_element,None) for name,schema in zip(names,schemas)})
        return schemas


class JsonSchema(AbstractSchema):
    json_schema : pydantic.Json

    @classmethod
    def from_str(cls,schema_str: str, schema_attributes : SchemaAttributes) -> JsonSchema:
        return cls(json_schema=schema_str,schema_attributes=schema_attributes)

    def to_json_schema(self) -> JsonSchema:
        """ Make a JSON Schema from this Schema """
        return self


    def to_output(self):
        """ return naked json schema """
        return  self.json_schema

class XmlSchema(AbstractSchema):
    xml_schema : str

    @classmethod
    def from_str(cls,schema_str: str, schema_attributes : SchemaAttributes) -> XmlSchema:
        return cls(xml_schema=schema_str,schema_attributes=schema_attributes)

    def to_output(self):
        """ return naked XML schema """
        return  self.xml_schema


SchemaFormat2Class = {
    SchemaFormat.json: JsonSchema,
     SchemaFormat.internal : PySchema,
     SchemaFormat.xsd : XmlSchema
}


def create_schema(s: str,sa: SchemaAttributes) -> AbstractSchema:
    sa = SchemaAttributes(**sa)
    sclass = SchemaFormat2Class.get(sa.format)
    if not sclass:
        raise errors.NotFound(f'Unknown schema format {sa.format}')
    schema = sclass.from_str(s,schema_attributes=sa)
    return schema