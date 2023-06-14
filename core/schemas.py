""" DDH Core AbstractSchema Models """
from __future__ import annotations

import abc
import enum
import typing
import weakref

import pydantic

from frontend import user_auth
from utils.pydantic_utils import DDHbaseModel

from . import (errors, keydirectory, keys, nodes, permissions, principals,
               versions, schema_network, trait)
from traits import restrictions, capabilities


import logging

logger = logging.getLogger(__name__)

# Global reference to singleton Schema Network:
SchemaNetwork: schema_network.SchemaNetworkClass = schema_network.SchemaNetworkClass()


@enum.unique
class Sensitivity(str, enum.Enum):
    """ Sensitivity, according to Fung et al., of use in export restrictions and anonymizatiodict[str, set[str]]n.
    """

    eid = 'explicit id'
    qid = 'quasi id'
    sa = 'sensitive attribute'
    nsa = 'non-sensitive attribute'


#    def __repr__(self): return self.value


@enum.unique
class Requires(str, enum.Enum):
    """ Schema Data requirements """

    one = 'one'
    few = 'few'
    specific = 'specific'
    many = 'many'


@enum.unique
class SchemaFormat(str, enum.Enum):
    internal = 'internal'
    json = 'json'
    xsd = 'xsd'


@enum.unique
class SchemaVariantUsage(str, enum.Enum):
    """ How a Schema should be used, allows management of alternate schemas and defaults. """
    recommended = 'recommended'
    supported = 'supported'
    obsolete = 'obsolete'


# Schema name in case of multiple schemas in the same space, e.g., ISO-20022 and Swift MT.
SchemaVariant = pydantic.constr(strip_whitespace=True, max_length=30, regex='[a-zA-Z0-9_-]+')


class MimeTypes(DDHbaseModel):
    """ Mime Types both for the schema itself and data conforming to the schema """
    of_schema: list[str] = pydantic.Field(
        description='Mimetype of the schema - taken from Schema if not provided.')
    of_data: list[str] = pydantic.Field(
        description='Mimetype of data - taken from Schema if not provided.')

    def for_fork(self, fork: keys.ForkType) -> list[str]:
        """ return mimetype for a fork """
        match fork:
            case keys.ForkType.data:
                mt = self.of_data
            case keys.ForkType.consents:
                mt = ['application/json']
            case keys.ForkType.schema:
                mt = self.of_schema
            case _:
                mt = ['*/*']
        return mt


T_PathFields = dict[str, set[str]]


class SchemaAttributes(DDHbaseModel):
    """ Attributes of the Schema, but not part of the Schema itself. """
    variant: SchemaVariant | None = pydantic.Field(
        default=None, description='Name of the variant, in case of multiple schemas in the same space, e.g., ISO-20022 and Swift MT')
    variant_usage: SchemaVariantUsage = pydantic.Field(
        SchemaVariantUsage.recommended, description="How this variant is used.")
    version: versions.Version = pydantic.Field(
        versions.Version(0), description="The version of this schema instance")
    requires: Requires | None = None
    mimetypes: MimeTypes | None = None
    references: dict[str, keys.DDHkeyRange] = {}  # TODO:#17 key should be DDHkey
    sensitivities: dict[Sensitivity, T_PathFields] = pydantic.Field(default={},
                                                                    description="Sensitivities by Sensitivity, schema key, set of fields. We cannot use DDHKey for schema key, as the dict is not jsonable.")
    transformers: trait.Transformers = trait.NoTransformers

    def add_reference(self, path: keys.DDHkey, reference: AbstractSchemaReference):
        print(f'SchemaAttributes.add_reference {path=}, {reference=}')
        self.references[str(path)] = reference.get_target()
        return

    def add_sensitivities(self, path: keys.DDHkey, sensitivities: dict[str, Sensitivity]):
        """ tranform path and {field : Sensitivity} to structure by Sensitivity, path, fields. """
        for field, s in sensitivities.items():
            self.sensitivities.setdefault(s, {}).setdefault(str(path), set()).add(field)
        return


class AbstractSchemaElement(DDHbaseModel, abc.ABC):
    """ An element within a Schema retrieved by key remainder  """

    @classmethod
    def to_schema(cls) -> AbstractSchema:
        """ create a Schema which contains this SchemaElement as its root """
        raise errors.SubClass

    @classmethod
    def create_from_elements(cls, key: keys.DDHkey | tuple | str, **elements: typing.Mapping[str, tuple[type, typing.Any]]) -> typing.Self:
        """ Create a named concrete SchemaElement from a Mapping of elements, which {name : (type,default)} """
        raise errors.SubClass

    @classmethod
    def store_as_schema(cls, ddhkey: keys.DDHkeyGeneric, schema_attributes: SchemaAttributes | None = None, parent: AbstractSchema | None = None) -> type[AbstractSchemaReference]:
        """ Replace this SchemaElement by a proper schema with attributes, store it, 
            and return the SchemaReference to it, which can be used like a SchemaElement.
        """
        ddhkey = ddhkey.ens()
        s = cls.to_schema()
        if schema_attributes:
            s.schema_attributes = schema_attributes
            s.update_schema_attributes()
        if parent:  # inherit restrictions
            s.schema_attributes.transformers = parent.schema_attributes.transformers.merge(
                s.schema_attributes.transformers)
        snode = nodes.SchemaNode(owner=principals.RootPrincipal,
                                 consents=AbstractSchema.get_schema_consents())
        keydirectory.NodeRegistry[ddhkey] = snode  # sets snode.key
        snode.add_schema(s)
        # now create reference
        vv = keys.DDHkeyVersioned0(ddhkey, variant=s.schema_attributes.variant, version=s.schema_attributes.version)
        schemaref = s.get_reference_class().create_from_key(ddhkey=vv.to_range())
        return schemaref

    @classmethod
    def resolve(cls, remainder: keys.DDHkey, principal: principals.Principal, q) -> dict:
        """ resolve on all subschemas, returning data.
            If schema provides data at its level, refine .resolve() and
            call super().resolve()
        """
        return {}

    @classmethod
    def insert_as_schema(cls, transaction, ddhkey: keys.DDHkeyGeneric, schema_attributes: SchemaAttributes | None = None):
        ddhkey = ddhkey.ens()
        parent, split = AbstractSchema.get_parent_schema(transaction, ddhkey)
        ref = cls.store_as_schema(ddhkey, schema_attributes, parent)
        return parent.insert_schema_ref(transaction, ddhkey, split, ref)

    @classmethod
    def extract_attributes(cls, path: keys.DDHkey, atts: SchemaAttributes):
        """ Extract attributes, may be used to modify schema.schema_attributes, 
            typically references or sensitivities.
        """
        return


class AbstractSchema(DDHbaseModel, abc.ABC, typing.Iterable):
    format_designator: typing.ClassVar[SchemaFormat] = SchemaFormat.internal
    schema_attributes: SchemaAttributes = pydantic.Field(
        default=SchemaAttributes(), descriptor="Attributes associated with this Schema")
    mimetypes: typing.ClassVar[MimeTypes | None] = None
    _w_container: weakref.ReferenceType[SchemaContainer] | None = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.update_schema_attributes()

    @abc.abstractmethod
    def __getitem__(self, key: keys.DDHkey, default=None, create_intermediate: bool = False) -> type[AbstractSchemaElement] | None:
        """ get Schema element at remainder key (within Schema only)
            create_intermediate is used to obtain parent and is used by .__setitem__() 
        """
        ...

    def __setitem__(self, key: keys.DDHkey, value: type[AbstractSchemaElement], create_intermediate: bool = True) -> type[AbstractSchemaElement] | None:
        pkey = key.up()
        parent = self.__getitem__(pkey, create_intermediate=create_intermediate)
        assert parent
        assert issubclass(value, AbstractSchemaElement)
        parent._add_fields(**{str(key[-1]): (value, None)})
        return parent

    @abc.abstractmethod
    def __iter__(self) -> typing.Iterator[tuple[keys.DDHkey, AbstractSchemaElement]]:
        ...

    @classmethod
    def get_reference_class(cls) -> type[AbstractSchemaReference]:
        """ get class of concrete AbstractSchemaReference associated with this concrete Schema.
            Return AbstractSchemaReference unless refined.
        """
        return AbstractSchemaReference

    @classmethod
    def __init_subclass__(cls):
        SchemaFormat2Class[cls.format_designator] = cls
        Class2SchemaFormat[cls] = cls.format_designator

    @property
    def container(self) -> SchemaContainer:
        assert self._w_container
        c = self._w_container()
        assert c is not None
        return c

    def update_schema_attributes(self):
        """ update .schema_attributes based on schema.
            updates .variant and mimetype by default, but can be refined. 
            The .variant name defaults to the name of the schema class.
        """
        if not self.schema_attributes.variant:  # variant name default is class name
            self.schema_attributes.variant = self.__class__.__name__
        self.update_mimetype()
        self.walk_elements()

    def update_mimetype(self):
        """ Take mimetype from Schema if provided and absent in schema_attributes """
        if not self.schema_attributes.mimetypes and self.mimetypes:
            self.schema_attributes.mimetypes = self.mimetypes

    def walk_elements(self):
        """ walk through the elements to gather attributes which are written into . schema_attributes """
        for path, element in self:
            element.extract_attributes(path, self.schema_attributes)
        return

    def parse(self, data: bytes) -> dict:
        """ Parse raw data, may raise errors.ParseError.
            Does not validate, this is done by a restriction. 
        """
        raise errors.SubClass

    def validate_data(self, data: dict, remainder: keys.DDHkey, no_extra: bool = True) -> dict:
        """ validate - called by restrictions.MustValidate """
        return data

    def after_schema_read(self, access: permissions.Access, transaction) -> AbstractSchema:
        """ Prepare Schema for get, returning this or modified schema """
        schema = self.expand_references()
        return schema

    def before_schema_write(self, access: permissions.Access, transaction) -> AbstractSchema:
        """ Prepare Schema for put, returning this or modified schema
            TODO: Schema checks:
                No shadowing - cannot insert into an existing schema, including into refs
                Reference update
                ref -> update referenced
                schema update -> ref
                uniform schema tree - all references must be in same schema repr

        """
        schema = self
        schema = self.schema_attributes.transformers.apply(schema, access, transaction, schema)
        return schema

    def after_data_read(self, access: permissions.Access, transaction, data):
        """ check data obtained through Schema; may be used to apply capabilities """
        data = self.schema_attributes.transformers.apply(self, access, transaction, data)
        return data

    def before_data_write(self, access: permissions.Access, transaction, data):
        """ check data against Schema; may be used to apply capabilities:
                Data version must correspond to a schema version
                LatestVersion: non-latest version data cannot be put unless upgrade exists
                UnderSchemaReference: data under schema reference only if schema reprs are compatible

        """
        data = self.schema_attributes.transformers.apply(self, access, transaction, data)
        return data

    def expand_references(self) -> AbstractSchema:
        """ Replace all references to other schemas by embedding the other schema into
            this schema. Works only if schemas have the same representation. 
        """
        if (refs := self.schema_attributes.references):
            logger.error('reference expansion not supported')
            return self
        else:
            return self

    def transform(self, path_fields: T_PathFields, selection: str, data, method, sensitivity, access, transaction, cache):
        """ transform data in place by applying method to path_fields. 
            selection is a str path selecting into the schema. 
            Must be overwritten if data is not a Python dictionary (compatible with Python and JSON)
        """
        for path in path_fields:
            if path.startswith(selection):  # is the selection within the path?

                remaining_path = path.split('.')[bool(selection)+selection.count('.'):]
                if not remaining_path: remaining_path = ['']  # we have selected a leaf
                for s in remaining_path:  # access sub-parts of DDHkey
                    if s:  # working at non-leaf
                        subdata = data.get(s)
                        if subdata is None:  # data is absent
                            break
                    else:  # leaf
                        subdata = data
                    # now working through fields, which may be lists:
                    for field in path_fields[path]:
                        if isinstance(subdata, list):  # iterate over list or tuple
                            for i, x in enumerate(subdata):
                                subdata[i][field] = method(x.get(field), path, field,
                                                           sensitivity, access, transaction, cache)
                        else:
                            subdata[field] = method(subdata.get(field), path, field,
                                                    sensitivity, access, transaction, cache)
                    # Merge back:
                    if s:
                        data[s] = subdata
                    else:
                        data = subdata

        return data

    @property
    def format(self) -> SchemaFormat:
        """ Schema format based on class """
        return Class2SchemaFormat[self.__class__]

    def to_json_schema(self):
        """ Make a JSON Schema from this Schema """
        raise NotImplemented('conversion to JSON schema not supported')

    @classmethod
    @abc.abstractmethod
    def from_str(cls, schema_str: str, schema_attributes: SchemaAttributes) -> AbstractSchema:
        ...

    def to_format(self, format: SchemaFormat):
        """ migrate schema to another format. 
            TODO: Do we really need or want this?
        """
        if format == self.format:
            return self.to_output()
        elif format.value == SchemaFormat.json:
            return self.to_json_schema().to_output()
        else:
            raise NotImplementedError(
                f'output format {format} not supported for {self.__class__.__name__}')

    @abc.abstractmethod
    def to_output(self) -> str:
        """ native output representation """
        ...

    def _add_fields(self, fields: dict):
        raise NotImplementedError('Field adding not supported in this schema')

    @staticmethod
    def get_schema_consents() -> permissions.Consents:
        """ Schema world read access consents """
        return permissions.Consents(consents=[permissions.Consent(grantedTo=[principals.AllPrincipal], withModes={permissions.AccessMode.read})])

    def insert_schema_ref(self, transaction, ddhkey: keys.DDHkey, split: int, schemaref: type[AbstractSchemaReference] | None = None) -> type[AbstractSchemaReference]:
        """ Add a schema reference to self (=parent schema); 
            if schemaref is not given, it is created pointing to ddhkey.
        """
        remainder = ddhkey.remainder(split)

        if not schemaref:  # create schema ref in parent's schema format:
            schemaref = self.get_reference_class().create_from_key(ddhkey=ddhkey)

        # now insert our schema into the parent's:
        self.__setitem__(remainder, schemaref, create_intermediate=True)
        return schemaref

    @staticmethod
    def get_parent_schema(transaction, ddhkey: keys.DDHkey) -> tuple[AbstractSchema, int]:
        """ get parent scheme of scheme at ddhkey, return [parent,split] """
        parent, key, split, node = SchemaContainer.get_node_schema_key(ddhkey.up(), transaction)
        if not parent:
            raise errors.NotFound(f'No parent for {ddhkey}')
        return parent, split

    @classmethod
    def create_schema(cls, s: str, format: SchemaFormat, sa: dict) -> AbstractSchema:
        """ Create Schema from a string repr, used by API to instantiate JsonSchema from DApps """
        sat = SchemaAttributes(**sa)
        sclass = SchemaFormat2Class.get(format)
        if not sclass:
            raise errors.NotFound(f'Unknown schema format {format}')
        schema = sclass.from_str(s, schema_attributes=sat)
        return schema


SchemaFormat2Class = {}
Class2SchemaFormat = {}


class AbstractSchemaReference(AbstractSchemaElement):
    ddhkey: typing.ClassVar[str]

    @classmethod
    @abc.abstractmethod
    def create_from_key(cls, ddhkey: keys.DDHkeyRange, name: str | None = None) -> typing.Type[AbstractSchemaReference]:
        ...

    @classmethod
    @abc.abstractmethod
    def get_target(cls) -> keys.DDHkeyRange:
        """ get target key """
        ...

    @classmethod
    def getURI(cls) -> pydantic.AnyUrl:
        return typing.cast(pydantic.AnyUrl, str(cls.get_target()))


class SchemaContainer(DDHbaseModel):
    """ Holds one or more Schemas according to their variant and version,
        keeps latest version in versions.Unspecified per variant
        and recommended as variant ''.
    """
    class Config:
        arbitrary_types_allowed = True  # for upgraders

    __slots__: typing.ClassVar[tuple] = ('__weakref__',)  # allow weak ref to here

    schemas_by_variant: dict[SchemaVariant | None,
                             dict[versions.Version, AbstractSchema]] = {}
    upgraders: dict[SchemaVariant, versions.Upgraders] = {}

    def __bool__(self):
        return self.default_schema is not None

    def add(self, key: keys.DDHkeyGeneric, schema: AbstractSchema):
        """ add a schema, considering its attributes. key is the Container's generic key. """
        key = key.without_variant_version()  # ensure key is generic
        sa = schema.schema_attributes
        assert sa
        sbv = self.schemas_by_variant.setdefault(sa.variant, {})
        sbv[sa.version] = schema
        default_version = sbv.get(versions.Unspecified)
        # nothing yet or newer:
        if not (default_version and sa.version < default_version.schema_attributes.version):
            sbv[versions.Unspecified] = schema  # new default version

        if sa.variant_usage == SchemaVariantUsage.recommended:  # latest recommended schema becomes default
            self.schemas_by_variant[''] = sbv
        SchemaNetwork.add_schema(key, schema.schema_attributes)
        schema._w_container = weakref.ref(self)  # keep a ref to the container
        return schema

    def get(self, variant: SchemaVariant = '', version: versions.Version = versions.Unspecified) -> AbstractSchema | None:
        """ get a specific schema """
        return self.schemas_by_variant.get(variant, {}).get(version)

    @property
    def default_schema(self):
        """ return default variant and latest version """
        return self.get()

    @staticmethod
    def get_node_schema_key(ddhkey: keys.DDHkey, transaction) -> tuple[AbstractSchema, keys.DDHkey, int, nodes.SchemaNode]:
        """ for a ddhkey, get the node, then get its schema and the fully qualified key with the schema variant 
            and version, and the split separting the schema and the key into the schema. 
            The key returned will have the fork and owner of the original key, except there is no owner when the fork is schema.
        """
        schema_ddhkey = ddhkey.ens()  # schema key to get the schema node
        ddhkey = schema_ddhkey if ddhkey.fork == keys.ForkType.schema else ddhkey  # but return only if schema fork is asked for
        snode, split = keydirectory.NodeRegistry.get_node(
            schema_ddhkey, nodes.NodeSupports.schema, transaction)
        if snode:
            assert isinstance(snode, nodes.SchemaNode)
            schema = snode.schemas.get_schema_key(schema_ddhkey)
            # build key with actual variant and version:
            fqkey = keys.DDHkey(ddhkey.key, specifiers=(
                ddhkey.fork, schema.schema_attributes.variant, schema.schema_attributes.version))
            return (schema, fqkey, split, snode)
        else:
            raise errors.NotFound(f'No schema node found for {ddhkey}')

    def get_schema_key(self, ddhkey: keys.DDHkeyVersioned) -> AbstractSchema:
        """ for a ddhkey, get its schema  """
        schema = self.get(ddhkey.variant, ddhkey.version)
        if not schema:
            raise errors.NotFound(f'No schema variant and version found for {ddhkey}')
        else:
            return schema

    @staticmethod
    def get_sub_schema(access: permissions.Access, transaction, create_intermediate: bool = False) -> AbstractSchema | None:
        schema = None
        parent_schema, access.ddhkey, split, snode = SchemaContainer.get_node_schema_key(
            access.ddhkey, transaction)
        if parent_schema:
            access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
                access.ddhkey.without_variant_version(), nodes.NodeSupports.schema, snode, transaction))
            remainder = access.ddhkey.remainder(split)
            schema_element = parent_schema.__getitem__(remainder, create_intermediate=create_intermediate)
            if schema_element:
                schema = schema_element.to_schema()
        return schema

    def fullfills(self, ddhkey: keys.DDHkey, version_constraint: versions.VersionConstraint) -> typing.Iterator[AbstractSchema]:
        """ return iterator of schemas that fulfill key and VersionConstraint """
        # cands is dict of either specified variant or None for default variant (which has key None):
        cands = self.schemas_by_variant.get(ddhkey.variant, {})
        return (schema for version, schema in cands.items() if version in version_constraint)

    def add_upgrader(self, variant: SchemaVariant, v_from: versions.Version, v_to: versions.Version, function: versions.Upgrader | None):
        upgraders = self.upgraders.setdefault(variant, versions.Upgraders())
        upgraders.add_upgrader(v_from, v_to, function)
