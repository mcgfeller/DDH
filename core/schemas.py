""" DDH Core AbstractSchema Models """
from __future__ import annotations

import abc
import enum
import typing

import pydantic
from frontend import user_auth
from utils.pydantic_utils import NoCopyBaseModel

from . import (errors, keydirectory, keys, nodes, permissions, principals,
               versions)


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


class MimeTypes(NoCopyBaseModel):
    """ Mime Types both for the schema itself and data conforming to the schema """
    of_schema: str = pydantic.Field(
        description='Mimetype of the schema - taken from Schema if not provided.')
    of_data: str = pydantic.Field(
        description='Mimetype of data - taken from Schema if not provided.')

    def for_fork(self, fork: keys.ForkType) -> str:
        """ return mimetype for a fork """
        match fork:
            case keys.ForkType.data:
                mt = self.of_data
            case keys.ForkType.consents:
                mt = 'application/json'
            case keys.ForkType.schema:
                mt = self.of_schema
            case _:
                mt = '*/*'
        return mt


class SchemaAttributes(NoCopyBaseModel):
    """ Attributes of the Schema, but not part of the Schema itself. """
    variant: SchemaVariant | None = pydantic.Field(
        default=None, description='Name of the variant, in case of multiple schemas in the same space, e.g., ISO-20022 and Swift MT')
    variant_usage: SchemaVariantUsage = pydantic.Field(
        SchemaVariantUsage.recommended, description="How this variant is used.")
    version: versions.Version = pydantic.Field(
        versions.Unspecified, description="The version of this schema instance")
    requires: Requires | None = None
    mimetypes: MimeTypes | None = None


class AbstractSchema(NoCopyBaseModel, abc.ABC):
    format_designator: typing.ClassVar[SchemaFormat] = SchemaFormat.internal
    schema_attributes: SchemaAttributes = pydantic.Field(
        default=SchemaAttributes(), descriptor="Attributes associated with this Schema")
    mimetypes: typing.ClassVar[MimeTypes | None] = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.update_schema_attributes()

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

    def update_mimetype(self):
        """ Take mimetype from Schema if provided and absent in schema_attributes """
        if not self.schema_attributes.mimetypes and self.mimetypes:
            self.schema_attributes.mimetypes = self.mimetypes

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

    def obtain(self, ddhkey: keys.DDHkey, split: int, create: bool = False) -> AbstractSchema | None:
        return None

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

    def add_fields(self, fields: dict):
        raise NotImplementedError('Field adding not supported in this schema')

    @staticmethod
    def get_schema_consents() -> permissions.Consents:
        """ Schema world read access consents """
        return permissions.Consents(consents=[permissions.Consent(grantedTo=[principals.AllPrincipal], withModes={permissions.AccessMode.read})])


SchemaFormat2Class = {}
Class2SchemaFormat = {}


class SchemaReference(NoCopyBaseModel):
    # TODO: Make version_required part of key
    ddhkey: typing.ClassVar[str]
    # variant: SchemaVariant = ''
    version_required: versions.VersionConstraint = pydantic.Field(
        default=versions.NoConstraint, description="Constrains the version of the target schema")


class SchemaContainer(NoCopyBaseModel):
    """ Holds one or more Schemas according to their variant and version,
        keeps latest version in versions.Unspecified per variant
        and recommended as variant ''.
    """

    schemas_by_variant: dict[SchemaVariant,
                             dict[versions.Version, AbstractSchema]] = {}

    def __bool__(self):
        return self.default_schema is not None

    def add(self, schema: AbstractSchema):
        """ add a schema, considering its attributes """
        sa = schema.schema_attributes
        sbv = self.schemas_by_variant.setdefault(sa.variant, {})
        sbv[sa.version] = schema
        default_version = sbv.get(versions.Unspecified)
        # nothing yet or newer:
        if not (default_version and sa.version < default_version.schema_attributes.version):
            sbv[versions.Unspecified] = schema  # new default version

        if sa.variant_usage == SchemaVariantUsage.recommended:  # latest recommended schema becomes default
            self.schemas_by_variant[''] = sbv
        return schema

    def get(self, variant: SchemaVariant = '', version: versions.Version = versions.Unspecified) -> AbstractSchema | None:
        """ get a specific schema """
        return self.schemas_by_variant.get(variant, {}).get(version)

    @property
    def default_schema(self):
        """ return default variant and latest version """
        return self.get()

    @staticmethod
    def get_node_schema_key(ddhkey: keys.DDHkey, transaction) -> tuple[AbstractSchema, keys.DDHkey]:
        """ for a ddhkey, get the node, then get its schema and the fully qualified schema key """
        snode, split = keydirectory.NodeRegistry.get_node(
            ddhkey, nodes.NodeSupports.schema, transaction)
        if snode:
            assert isinstance(snode, nodes.SchemaNode)
            return snode.schemas.get_schema_key(ddhkey)
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


def create_schema(s: str, format: SchemaFormat, sa: SchemaAttributes) -> AbstractSchema:
    sa = SchemaAttributes(**sa)
    sclass = SchemaFormat2Class.get(format)
    if not sclass:
        raise errors.NotFound(f'Unknown schema format {format}')
    schema = sclass.from_str(s, schema_attributes=sa)
    return schema
