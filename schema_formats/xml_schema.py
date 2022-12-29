""" JSON Schema Format """
from __future__ import annotations
import typing
import pydantic


from core import schemas, keys, errors


class XmlSchemaElement(schemas.AbstractSchemaElement):

    @classmethod
    def to_schema(cls) -> XmlSchema:
        return XmlSchema(xml_schema=cls)


class XmlSchema(schemas.AbstractSchema):
    format_designator: typing.ClassVar[schemas.SchemaFormat] = schemas.SchemaFormat.xsd
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema='application/xsd', of_data='application/xml')
    xml_schema: str

    def __getitem__(self, key: keys.DDHkey, default=None) -> type[XmlSchemaElement] | None:
        raise errors.SubClass

    def __setitem__(self, key: keys.DDHkey, value: type[XmlSchemaElement], create_intermediate: bool = True) -> type[XmlSchemaElement] | None:
        raise errors.SubClass

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> XmlSchema:
        return cls(xml_schema=schema_str, schema_attributes=schema_attributes)

    def to_output(self):
        """ return naked XML schema """
        return self.xml_schema
