""" DDH Core AbstractSchema Models """
from __future__ import annotations

import abc
import enum
import typing

import pydantic
from frontend import user_auth
from utils.pydantic_utils import DDHbaseModel

from . import (errors, keydirectory, keys, nodes, permissions, principals,
               versions, capabilities, schema_network)

import logging

logger = logging.getLogger(__name__)

# Global reference to singleton Schema Network:
SchemaNetwork: schema_network.SchemaNetworkClass = schema_network.SchemaNetworkClass()


@enum.unique
class Sensitivity(str, enum.Enum):
    """ Sensitivity, according to Fung et al., of use in export restrictions and anonymization.
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


class SchemaAttributes(DDHbaseModel):
    """ Attributes of the Schema, but not part of the Schema itself. """
    variant: SchemaVariant | None = pydantic.Field(
        default=None, description='Name of the variant, in case of multiple schemas in the same space, e.g., ISO-20022 and Swift MT')
    variant_usage: SchemaVariantUsage = pydantic.Field(
        SchemaVariantUsage.recommended, description="How this variant is used.")
    version: versions.Version = pydantic.Field(
        versions.Unspecified, description="The version of this schema instance")
    requires: Requires | None = None
    mimetypes: MimeTypes | None = None
    references: dict[keys.DDHkey, keys.DDHkey] = {}
    sensitivities: dict[Sensitivity, dict[str, set[str]]] = pydantic.Field(default={},
                                                                           description="Sensitivities by Sensitivity, schema key, set of fields. We cannot use DDHKey for schema key, as the dict is not jsonable.")
    capabilities: set[capabilities.Capabilities] = set()

    def add_reference(self, path: keys.DDHkey, reference: AbstractSchemaReference):
        self.references[path] = reference.get_target()
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
    def store_as_schema(cls, ddhkey: keys.DDHkey, schema_attributes: SchemaAttributes | None = None) -> type[AbstractSchemaReference]:
        """ Replace this PySchemaElement by a proper schema with attributes, store it, 
            and return the PySchemaReference to it, which can be used like a PySchemaElement.
        """
        s = cls.to_schema()
        if schema_attributes: s.schema_attributes = schema_attributes
        snode = nodes.SchemaNode(owner=principals.RootPrincipal,
                                 consents=AbstractSchema.get_schema_consents())
        keydirectory.NodeRegistry[ddhkey] = snode  # sets snode.key
        snode.add_schema(s)
        # now create reference
        schemaref = s.get_reference_class().create_from_key(ddhkey=ddhkey)
        return schemaref

    @classmethod
    def insert_as_schema(cls, transaction, ddhkey: keys.DDHkey, schema_attributes: SchemaAttributes | None = None):
        ref = cls.store_as_schema(ddhkey, schema_attributes)
        return AbstractSchema.insert_schema_ref(transaction, ddhkey, ref)

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
        return schema

    def after_data_read(self, access: permissions.Access, transaction, data):
        """ check data obtained through Schema; may be used to apply capabilities """
        data = self.apply_capabilities(access, transaction, data)
        return data

    def before_data_write(self, access: permissions.Access, transaction, data):
        """ check data against Schema; may be used to apply capabilities:
                Data version must correspond to a schema version
                non-latest version data cannot be put unless upgrade exists
                data under schema reference only if schema reprs are compatible

        """
        data = self.apply_capabilities(access, transaction, data)
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

    def apply_capabilities(self, access, transaction, data):
        caps = self.select_capabilities(access, transaction, data)
        for cap in caps:
            data = cap.apply(self, access, transaction, data)
        return data

    def select_capabilities(self, access, transaction, data) -> typing.Iterable[capabilities.SchemaCapability]:
        # join the capabilities from each mode:
        required_capabilities = capabilities.SchemaCapability.capabilities_for_modes(access.modes)
        missing = required_capabilities - self.schema_attributes.capabilities
        if missing:
            raise errors.CapabilityMissing(f"Schema {self} does not support required capabilities; missing {missing}")
        return self.schema_attributes.capabilities.intersection(required_capabilities)

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

    @staticmethod
    def insert_schema_ref(transaction, ddhkey: keys.DDHkey, schemaref: type[AbstractSchemaReference] | None = None) -> type[AbstractSchemaReference]:
        """ Add a schema reference to its parent schema; 
            if schemaref is not given, it is created pointing to ddhkey.
        """
        ddhkey = ddhkey.ensure_fork(keys.ForkType.schema)
        # get a parent scheme to hook into:
        parent, key, remainder, node = SchemaContainer.get_node_schema_key(ddhkey.up(), transaction)
        if not parent:
            raise errors.NotFound(f'No parent for {ddhkey}')

        if not schemaref:  # create schema ref in parent's schema format:
            schemaref = parent.get_reference_class().create_from_key(ddhkey=ddhkey)

        # now insert our schema into the parent's:
        parent.__setitem__(remainder, schemaref, create_intermediate=True)
        return schemaref

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
    # TODO: Make version_required part of key
    ddhkey: typing.ClassVar[str]
    # variant: SchemaVariant = ''
    version_required: versions.VersionConstraint = pydantic.Field(
        default=versions.NoConstraint, description="Constrains the version of the target schema")

    @classmethod
    @abc.abstractmethod
    def create_from_key(cls, ddhkey: keys.DDHkey, name: str | None = None) -> typing.Type[AbstractSchemaReference]:
        ...

    @classmethod
    @abc.abstractmethod
    def get_target(cls) -> keys.DDHkey:
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

    schemas_by_variant: dict[SchemaVariant | None,
                             dict[versions.Version, AbstractSchema]] = {}

    def __bool__(self):
        return self.default_schema is not None

    def add(self, key: keys.DDHkey, schema: AbstractSchema):
        """ add a schema, considering its attributes """
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
        SchemaNetwork.add_schema(key, schema.schema_attributes)  # TODO: Add schema to network
        return schema

    def get(self, variant: SchemaVariant = '', version: versions.Version = versions.Unspecified) -> AbstractSchema | None:
        """ get a specific schema """
        return self.schemas_by_variant.get(variant, {}).get(version)

    @property
    def default_schema(self):
        """ return default variant and latest version """
        return self.get()

    @staticmethod
    def get_node_schema_key(ddhkey: keys.DDHkey, transaction) -> tuple[AbstractSchema, keys.DDHkey, keys.DDHkey, nodes.SchemaNode]:
        """ for a ddhkey, get the node, then get its schema and the fully qualified schema key, and the remainder """
        snode, split = keydirectory.NodeRegistry.get_node(
            ddhkey, nodes.NodeSupports.schema, transaction)
        if snode:
            assert isinstance(snode, nodes.SchemaNode)
            return snode.schemas.get_schema_key(ddhkey)+(ddhkey.remainder(split), snode)
        else:
            raise errors.NotFound(f'No schema node found for {ddhkey}')

    def get_schema_key(self, ddhkey: keys.DDHkey) -> tuple[AbstractSchema, keys.DDHkey]:
        """ for a ddhkey, get its schema and the fully qualified schema key """
        schema = self.get(ddhkey.variant, ddhkey.version)
        if schema:
            fqkey = keys.DDHkey(ddhkey.key, specifiers=(
                ddhkey.fork, schema.schema_attributes.variant, schema.schema_attributes.version))
            return (schema, fqkey)
        else:
            raise errors.NotFound(f'No schema variant and version found for {ddhkey}')

    @staticmethod
    def get_sub_schema(access: permissions.Access, transaction, create_intermediate: bool = False) -> AbstractSchema | None:
        schema = None
        parent_schema, access.ddhkey, remainder, snode = SchemaContainer.get_node_schema_key(
            access.ddhkey, transaction)
        if parent_schema:
            access.raise_if_not_permitted(keydirectory.NodeRegistry._get_consent_node(
                access.ddhkey.without_variant_version(), nodes.NodeSupports.schema, snode, transaction))
            schema_element = parent_schema.__getitem__(remainder, create_intermediate=create_intermediate)
            if schema_element:
                schema = schema_element.to_schema()
        return schema

    def fullfills(self, ddhkey: keys.DDHkey, version_constraint: versions.VersionConstraint) -> typing.Iterator[AbstractSchema]:
        """ return iterator of schemas that fulfill key and VersionConstraint """
        # cands is dict of either specified variant or None for default variant (which has key None):
        cands = self.schemas_by_variant.get(ddhkey.variant, {})
        return (schema for version, schema in cands.items() if version in version_constraint)
