""" Pydantic utilities and superclasses """

from __future__ import annotations
import pydantic
import typing


class DDHbaseModel(pydantic.BaseModel):
    """ Default Model behavior
    """
    class Config:
        """ This forbids wrong keywords, preventing silly mistakes when defaulted
            attributes are not set.
        """
        extra = 'forbid'
        underscore_attrs_are_private = True
        copy_on_model_validation = 'none'  # see https://github.com/pydantic/pydantic/pull/2193

    # @classmethod
    # def validate(cls: typing.Type[pydantic.BaseModel], value: typing.Any) -> pydantic.BaseModel:
    #     if isinstance(value, cls):
    #         return value # don't copy!
    #     else:
    #         return super().validate(value)

    @classmethod
    def add_fields(cls, **field_definitions: typing.Any):
        """ Add fields in-place https://github.com/samuelcolvin/pydantic/issues/1937 """
        new_fields: dict[str, pydantic.fields.ModelField] = {}
        new_annotations: dict[str, type | None] = {}

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
                assert not issubclass(
                    f_def, pydantic.BaseModel), 'pass tuple or default value as single item (not model!)'
                f_annotation, f_value = None, f_def

            if f_annotation:
                new_annotations[f_name] = f_annotation

            new_fields[f_name] = pydantic.fields.ModelField.infer(
                name=f_name, value=f_value, annotation=f_annotation, class_validators=None, config=cls.__config__)

        cls.__fields__.update(new_fields)
        cls.__annotations__.update(new_annotations)
        cls.__schema_cache__.clear()
        return
