""" DDH Core Models """
from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin

class NoCopyBaseModel(pydantic.BaseModel):
    """ https://github.com/samuelcolvin/pydantic/issues/1246
        https://github.com/samuelcolvin/pydantic/blob/52af9162068a06eed5b84176e987a534f6d9126a/pydantic/main.py#L574-L575
    """
    class Config:
        """ This forbids wrong keywords, preventing silly mistakes when defaulted
            attributes are not set.
        """
        extra = 'forbid'
        underscore_attrs_are_private = True

    @classmethod
    def validate(cls: typing.Type[pydantic.BaseModel], value: typing.Any) -> pydantic.BaseModel:
        if isinstance(value, cls):
            return value # don't copy!
        else:
            return super().validate(value) 

    @classmethod
    def add_fields(cls, **field_definitions: typing.Any):
        """ Add fields in-place https://github.com/samuelcolvin/pydantic/issues/1937 """
        new_fields: dict[str, pydantic.fields.ModelField] = {}
        new_annotations: dict[str, typing.Optional[type]] = {}

        for f_name, f_def in field_definitions.items():
            if isinstance(f_def, tuple):
                try:
                    f_annotation, f_value = f_def
                except ValueError as e:
                    raise Exception(
                        'field definitions should either be a tuple of (<type>, <default>) or just a '
                        'default value, unfortunately this means tuples as '
                        'default values are not allowed'
                    ) from e
            else:
                f_annotation, f_value = None, f_def

            if f_annotation:
                new_annotations[f_name] = f_annotation

            new_fields[f_name] = pydantic.fields.ModelField.infer(name=f_name, value=f_value, annotation=f_annotation, class_validators=None, config=cls.__config__)

        cls.__fields__.update(new_fields)
        cls.__annotations__.update(new_annotations)
        cls.__schema_cache__.clear()
        return


class Principal(NoCopyBaseModel):
    """ Abstract identification of a party """

    id : str


AllPrincipal = Principal(id='_all_')
RootPrincipal = Principal(id='DDH')




@enum.unique
class AccessMode(str,enum.Enum):
    """ Access modes, can be used as list. 
        We cannot use enum.Flag (which could be added), as pydantic doesn't support exporting / importing it as strings
    """
    read = 'read'
    protected = 'protected' # flag with read and write, mandatory if consented for write
    write = 'write'
    anonymous = 'anonymous'
    pseudonym = 'pseudonym'
    schema_read = 'schema_read'
    schema_write = 'schema_write'    
    consent_read = 'consent_read'
    consent_write = 'consent_write'

    @classmethod
    def check(cls,requested :set[AccessMode], consented : set[AccessMode]) -> tuple[bool,str]:
        """ Check wether requsted modes are permitted by consented modes.
            There are two conditions:
            1.  All requested modes must be in consented modes; .RequiredModes do not count as
                consented.
            2.  If a mode in .RequiredModes is consented, it must be present in requested. 

        """
        # 1:
        for req in requested:
            if req not in consented and req not in AccessMode.RequiredModes :
                return False,f'requested mode {req} not in consented modes {", ".join(consented)}.'

        # 2:
        required_modes = consented.intersection(AccessMode.RequiredModes) # all modes required by our consent
        for miss in required_modes - requested: # but not requested
            if m:= AccessMode.RequiredModes[miss]: # specific for a requested mode only?
                if m.isdisjoint(requested): # yes, but this mode is not requested, so check next miss
                    continue
            return False,f'Consent requires {miss} mode in request, but only {", ".join(requested)} requested.' 
        return True,'ok, with required modes' if required_modes else 'ok, no restrictions'

# modes that need to be specified explicity in requested when consented. If value is a set, the requirement only applies to the value modes:
AccessMode.RequiredModes = {AccessMode.anonymous : None, AccessMode.pseudonym : None, AccessMode.protected : {AccessMode.write}} 


class User(Principal):
    """ Concrete user, may login """
       
    name : str 
    email : typing.Optional[pydantic.EmailStr] = None
    created_at : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now

class DAppId(Principal):
    """ The identification of a DApp. We use a Principal for now. """

    name : str



class Consent(NoCopyBaseModel):
    """ Consent to access a ressource denoted by DDHkey.
    """
    grantedTo : list[Principal]
    withApps : set[DAppId] = set()
    withModes : set[AccessMode]  = {AccessMode.read}


    def check(self,access : Access, _principal_checked=False) -> tuple[bool,str]:
        """ check access and return boolean and text explaining why it's not ok.
            If _principal_checked is True, applicable consents with correct principals 
            are checked, hence we don't need to double-check.
        """
        if (not _principal_checked) and self.grantedTo != AllPrincipal and access.principal not in self.grantedTo:
            return False,f'Consent not granted to {access.principal}'
        if self.withApps:
            if access.byDApp:
                if access.byDApp not in self.withApps:
                    return False,f'Consent not granted to DApp {access.byDApp}'
            else:
                return False,f'Consent granted to DApps; need an DApp id to access'
        
        ok,txt = AccessMode.check(access.modes,self.withModes)
        if not ok:
            return False,txt

        return True,'Granted by Consent; '+txt

class Consents(NoCopyBaseModel):
    """ Multiple Consents
    """
    consents : list[Consent] = []
    _byPrincipal : dict[str,list[Consent]] = {}

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self._byPrincipal = {} # for easier lookup
        for consent in self.consents:
            for principal in consent.grantedTo:
                cl = self._byPrincipal.setdefault(principal.id,[])
                cl.append(consent)
        return

    def applicable_consents(self,principal : Principal ) -> list[Consents]:
        """ return list of Consents for this principal """
        return self._byPrincipal.get(principal.id,[]) + self._byPrincipal.get(AllPrincipal.id,[])


    def check(self,access : Access) -> tuple[bool,typing.Optional[Consent],str]:
        msg = 'no consent'
        consent = None
        for consent in self.applicable_consents(access.principal):
            ok,msg = consent.check(access,_principal_checked=True)
            if ok:
                return ok,consent,msg
        else:
            return False,consent,msg

class _RootType(str):
    """ Singleton root marker """
    def __repr__(self):
        return '<Root>'

    def __str__(self):
        return ''

class DDHkey(NoCopyBaseModel):
    """ A key identifying a DDH ressource. DDHkey is decoupled from any access, storage, etc.,
    """
    
    key : tuple
    node: Node = None

    Delimiter : typing.ClassVar[str] = '/'
    Root : typing.ClassVar[_RootType] = _RootType(Delimiter)

    def __init__(self,key : typing.Union[tuple,list,str], node :  typing.Optional['Node'] = None):
        """ Convert key string into tuple, eliminate empty segments, and set root to self.Root """
        if isinstance(key,str):
            key = key.split(self.Delimiter)
        if len(key) == 0:
            key = () # ensure tuple
        elif not key[0]: # replace root delimiter with root object
            key = (self.Root,)+tuple(filter(None,key[1:]))
        else:
            key = tuple(filter(None,key))
        super().__init__(key=key,node=node)
        return 

    def __str__(self) -> str:
        return self.Delimiter.join(map(str,self.key))

    def __repr__(self) -> str:
        return f'DDHkey({self.Delimiter.join(map(str,self.key))})'

    def __iter__(self) -> typing.Iterator:
        """ Iterate over key """
        return iter(self.key)

    def __getitem__(self,ix) -> typing.Union[tuple,str]:
        """ get part of key, str if ix is integer, tuple if slice """
        return self.key.__getitem__(ix)


    def up(self) -> typing.Optional['DDHkey']:
        """ return key up one level, or None if at top """
        upkey = self.key[:-1]
        if upkey:
            return self.__class__(upkey)
        else: 
            return None

    def split_at(self,split : int) -> typing.Tuple[DDHkey,DDHkey]:
        """ split the key into two DDHkeys at split """
        return self.__class__(self.key[:split]),self.__class__(self.key[split:])

    def ensure_rooted(self) -> DDHkey:
        """ return a DHHkey that is rooted """
        if not self.key[0] == self.Root:
            return self.__class__((self.Root,)+self.key)
        else:
            return self





class Access(NoCopyBaseModel):
    """ This is a loggable Access Request, which may or may not get fulfilled.
        Use .permitted() to check whether this request is permitted. 
    """
    ddhkey:    DDHkey
    principal: Principal
    byDApp:    typing.Optional[DAppId] = None
    modes:      set[AccessMode]  = {AccessMode.read}
    time:      datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now
    
    def permitted(self) -> tuple[bool,typing.Optional[Consent],str]:
        """ checks whether access is permitted, returning (bool,required flags,applicable consent,explanation text)
        """
        onode,split = NodeRegistry.get_node(self.ddhkey,NodeType.owner)
        if not onode:
            return False,None,f'No owner node found for key {self.ddhkey}'
        elif onode.owner == self.principal:
            return True,None,'Node owned by principal'
        else:
            if onode.consents: # onode has consents, use it
                consents : Consents = onode.consents
            else: # obtain from consents node
                cnode,split = NodeRegistry.get_node(self.ddhkey,NodeType.consents) 
                if cnode:
                    consents = typing.cast(Consents,cnode.consents)  # consent is not None by get_node
                else:
                    return False,None,f'Owner is not accessor, and no consent node found for key {self.ddhkey}'
            ok,consent,msg = consents.check(self) # check consents
            return  ok,consent,msg

    
    def audit_record(self) -> dict:
        return {}

class SchemaElement(NoCopyBaseModel): 
    """ A Pydantic Schema class """

    @classmethod 
    def descend_path(cls,path: DDHkey) -> typing.Optional[typing.Type[SchemaElement]]:
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


class SchemaReference(SchemaElement):

    ddhkey : typing.ClassVar[str] 

    class Config:
        @staticmethod
        def schema_extra(schema: dict[str, typing.Any], model: typing.Type[SchemaReference]) -> None:
            schema['properties']['dep'] =  {'$ref': model.getURI()}
            return

    # @classmethod
    # def __modify_schema__(cls, field_schema):
    #     print(field_schema)
    #     field_schema['@ref'] = cls.getURI()
    #     return 

    @classmethod
    def getURI(cls) -> pydantic.AnyUrl:
        return typing.cast(pydantic.AnyUrl,str(cls.__fields__['ddhkey'].default))

    @classmethod
    def create_from_key(cls,name: str, ddhkey : str) -> typing.Type[SchemaReference]:
        m = pydantic.create_model(name,__base__ = cls,ddhkey = (DDHkey,ddhkey))
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


    def obtain(self,ddhkey: DDHkey,split: int) -> typing.Optional[Schema]:
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

    def obtain(self,ddhkey: DDHkey,split: int) -> typing.Optional[Schema]:
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




@enum.unique
class NodeType(str,enum.Enum):
    """ Types of Nodes, marked by presence of attribute corresponding with enum value """

    owner = 'owner'
    nschema = 'nschema'
    consents = 'consents'
    data = 'data'
    execute = 'execute'


class Node(NoCopyBaseModel):

    owner: Principal
    consents : typing.Optional[Consents] = None
    nschema : typing.Optional[Schema] =  pydantic.Field(alias='schema')
    key : typing.Optional[DDHkey] = None

    def __str__(self):
        """ short representation """
        return f'Node(key={self.key!s},owner={self.owner.id})'


    def get_sub_schema(self, ddhkey: DDHkey,split: int, schema_type : str = 'json') -> typing.Optional[Schema]:
        """ return schema based on ddhkey and split """
        schema = typing.cast(Schema,self.nschema)
        schema = schema.obtain(ddhkey,split)
        return schema


class ExecutableNode(Node):
    def execute(self,  user: Principal, q : typing.Optional[str] = None):
        return {}


class DAppNode(ExecutableNode):
    """ node managed by a DApp """
    ...

class StorageNode(ExecutableNode):
    """ node with storage on DDH """
    ...

DDHkey.update_forward_refs() # Now Node is known

class _NodeRegistry:
    """ Preliminary holder of nodes """

    nodes_by_key : dict[tuple,Node]

    def __init__(self):
        self.nodes_by_key = {}

    def __setitem__(self,key : DDHkey, node: Node):
        self.nodes_by_key[key.key] = node
        node.key = key

    def __getitem__(self,key : DDHkey) -> typing.Optional[Node]:
        return self.nodes_by_key.get(key.key,None) 

    def get_next_node(self,key : typing.Optional[DDHkey]) -> typing.Iterator[typing.Tuple[Node,int]]:
        """ Generating getting next node walking up the tree from key.
            Also indicates at which point the DDHkey is to be split so the first part is the
            path leading to the Node, the 2nd the rest. 
            """
        split = len(key.key) # where to split: counting backwards from the end. 
        while key:
            node =  self[key]
            key = key.up() 
            split -= 1
            if node:
                yield node,split+1
        else:
            return

    def get_node(self,key : DDHkey,node_type : NodeType) -> typing.Tuple[typing.Optional[Node],int]:
        """ get closest (upward-bound) node which has nonzero attribute """
        node,split = next(( (node,split) for node,split in self.get_next_node(key) if getattr(node,node_type.value,None)),(None,-1))
        return node,split
    

NodeRegistry = _NodeRegistry()

NodeRegistry[DDHkey((DDHkey.Root,))] = Node(owner=RootPrincipal)

class AccessError(Exception):
    def __init__(self,text,consent=None):
        self.text = text

    def __str__(self):
        return self.text 

def get_schema(access : Access, schemaformat: SchemaFormat = SchemaFormat.json) -> typing.Optional[typing.Any]:
    """ Service utility to retrieve a Schema and return it in the desired format.
        Returns None if no schema found.
    """
    formatted_schema = None # in case of not found. 
    ddhkey = access.ddhkey.ensure_rooted()
    snode,split = NodeRegistry.get_node(ddhkey,NodeType.nschema) # get applicable schema node
    ok,consent,text = access.permitted()
    if not ok:
       return None
    
    if snode:
        schema = snode.get_sub_schema(ddhkey,split)
        if schema:
            formatted_schema = schema.format(schemaformat)
    return formatted_schema


def get_data(access : Access, q : typing.Optional[str] = None) -> typing.Any:
    """ Service utility to retrieve data and return it in the desired format.
        Returns None if no data found.
    """
    ddhkey = access.ddhkey.ensure_rooted()
    enode,split = NodeRegistry.get_node(ddhkey,NodeType.execute)
    enode = typing.cast(ExecutableNode,enode)
    data = enode.execute(access.principal, q)
    return data