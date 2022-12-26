""" JSON Schema Format """
from __future__ import annotations
import typing
import pydantic


from core import schemas, keys


class XmlSchema(schemas.AbstractSchema):
    format_designator: typing.ClassVar[schemas.SchemaFormat] = schemas.SchemaFormat.xsd
    mimetypes: typing.ClassVar[schemas.MimeTypes] = schemas.MimeTypes(
        of_schema='application/xsd', of_data='application/xml')
    xml_schema: str

    @classmethod
    def from_str(cls, schema_str: str, schema_attributes: schemas.SchemaAttributes) -> XmlSchema:
        return cls(xml_schema=schema_str, schema_attributes=schema_attributes)

    def to_output(self):
        """ return naked XML schema """
        return self.xml_schema
