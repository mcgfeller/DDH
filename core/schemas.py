""" DDH Core Schema Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel

from . import keys,permissions,errors

class SchemaElement(NoCopyBaseModel): 
    """ A Pydantic Schema class """

    @classmethod 
    def descend_path(cls,path: keys.DDHkey) -> typing.Optional[typing.Type[SchemaElement]]:
        """ Travel down SchemaElement along path using some Pydantic implementation details.
            If a path segment is not found, return None.
            If a path ends with a simple datatype, we return its parent.  
        """ 
        current = cls # before we descend path, this cls is at the current level 
        pathit = iter(path) # so we can peek whether we're at end
        for segment in pathit:
            mf = current.__fields__.get(segment,None) # look up one segment of path, returning ModelField
            if mf is None:
                return None
            else: 
                assert isinstance(mf,pydantic.fields.ModelField)
                if issubclass(mf.type_,SchemaElement):
                    current = mf.type_ # this is the next Pydantic class
                else: # we're at a leaf, return
                    if next(pathit,None) is None: # path ends here
                        break 
                    else: # path continues beyond this point, so this is not found
                        return None 
        return current

    @classmethod
    def list_of_ids(cls) -> typing.Optional[str]:
        """ return name of identifying element if this SchemaElement is a
            list with an Element with id : Principal.
        """
        return None


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
            principals = [n for n,t in innerclass.__fields__.items() if issubclass(t.type_,permissions.Principal)]
            if principals:
                if 'id' in principals:
                    return (innerclass,sub.__origin__,'id')
                else:
                    return (innerclass,sub.__origin__,principals[0])
            else:
                return (innerclass,sub.__origin__,None)
        else:
            raise errors.DAppError(f'Cannot understand element {subname}={sub} in {cls}')

    def get_resolver(self,  selection: keys.DDHkey,access: permissions.Access, q):
        ids : typing.Dict[type,typing.Dict[str,list]] = {} # {class : {idattr : [id,...]}}
        entire_selection = selection
        schema = self.__class__
        while len(selection.key):
            next_key,remainder = selection.split_at(1) # next level
            schema,container,idattr = schema.get_subschema_class(next_key)
            if not schema:
                raise errors.NotFound(f'Invalid key {next_key} in {entire_selection}') 
            if container:
                sel,remainder = remainder.split_at(1) # next level is ids
                if idattr:
                    principals = permissions.Principal.get_principals(str(sel)) # existing prinicpals (may raise NotFound)
                    for principal in principals:
                        p_access = access.copy() # create an access record for actual key (maybe optimize if it's just one key?)
                        p_key = keys.DDHkey(key=access.ddhkey[:-(len(remainder.key)+1)]+(principal.id,)+remainder.key)
                        p_access.ddhkey = p_key
                        ok,consent,text = p_access.permitted(owner=principal) # here we check the consent
                        if not ok:
                            raise errors.AccessError(text)
                    ids.setdefault(schema,{})[idattr] = principals 
            resolver = getattr(schema,'resolve',None)
            if resolver:
                res = resolver(remainder,ids, q)
                return res
            selection = remainder
        else: # there is no resolver so far, we cannot grab this without a further segment:
            raise errors.NotFound(f'Incomplete key: {entire_selection}')


class SchemaReference(SchemaElement):

    ddhkey : typing.ClassVar[str] 

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



class Schema(NoCopyBaseModel,abc.ABC):

    @abc.abstractmethod
    def to_py_schema(self) -> PySchema:
        """ return an equivalent Schema as PySchema """

    @classmethod
    @abc.abstractmethod   
    def from_schema(cls,schema: Schema) -> Schema:
        """ return schema in this class """
        ...


    def obtain(self,ddhkey: keys.DDHkey,split: int) -> typing.Optional[Schema]:
        return None

    def format(self,format : SchemaFormat):
        dschema = SchemaFormats[format.value].from_schema(self)
        return dschema.to_output()

    def to_output(self):
        return self

    def add_fields(self,fields : dict):
        raise NotImplementedError('Field adding not supported in this schema')



class PySchema(Schema):
    """ A Schema in Pydantic Python, containing a SchemaElement """ 
    schema_element : typing.Type[SchemaElement]

    def obtain(self,ddhkey: keys.DDHkey,split: int) -> typing.Optional[Schema]:
        """ obtain a schema for the ddhkey, which is split into the key holding the schema and
            the remaining path. 
        """
        khere,kremainder = ddhkey.split_at(split)
        if kremainder:
            schema_element = self.schema_element.descend_path(kremainder)
            if schema_element:
                s = PySchema(schema_element=schema_element)
            else: s = None # not found
        else:
            s = self
        return s

    def to_py_schema(self) -> PySchema:
        """ we're a PySchema, so return self """
        return self

    @classmethod
    def from_schema(cls,schema: Schema) -> PySchema:
        return schema.to_py_schema()

    def to_output(self):
        """ dict representation of internal schema """
        return  self.schema_element.schema()

    def add_fields(self,fields : dict[str,tuple]):
        """ Add the field in dict """
        self.schema_element.add_fields(**fields)


class JsonSchema(Schema):
    json_schema : pydantic.Json

    @classmethod
    def from_schema(cls,schema: Schema) -> JsonSchema:
        """ Make a JSON Schema from any Schema """
        if isinstance(schema,cls):
            return typing.cast(JsonSchema,schema)
        else:
            pyschema = schema.to_py_schema()
            return cls(json_schema=pyschema.schema_element.schema_json())

    def to_py_schema(self) -> PySchema:
        """ create Python Schema """
        raise NotImplementedError('not supported')

    def to_output(self):
        """ return naked json schema """
        return  self.json_schema

SchemaFormats = {
    'json': JsonSchema,
    'internal' : PySchema,
}
# corresponding enum: 
SchemaFormat = enum.Enum('SchemaFormat',[(k,k) for k in SchemaFormats])  # type: ignore # 2nd argument with list form not understood

