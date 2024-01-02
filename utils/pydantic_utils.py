""" Pydantic utilities and superclasses """

from __future__ import annotations
import pydantic
import typing
import datetime

global CV
CV = typing.ClassVar


class DDHbaseModel(pydantic.BaseModel):
    """ Default Model behavior
    """
    # TODO[pydantic]: The following keys were removed: `underscore_attrs_are_private`, `copy_on_model_validation`.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    model_config = pydantic.ConfigDict(extra='forbid')

    # @classmethod
    # def validate(cls: typing.Type[pydantic.BaseModel], value: typing.Any) -> pydantic.BaseModel:
    #     if isinstance(value, cls):
    #         return value # don't copy!
    #     else:
    #         return super().validate(value)

    @classmethod
    def _add_fields(cls, **field_definitions: typing.Any):
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

            # new_fields[f_name] = pydantic.fields.ModelField.infer(
            #     name=f_name, value=f_value, annotation=f_annotation, class_validators=None, config=cls.__config__)
            new_fields[f_name] = pydantic.fields.FieldInfo(annotation=f_annotation)

        # FIXME #32: This is highly dubious
        cls.model_fields.update(new_fields)
        cls.__annotations__.update(new_annotations)
        cls.model_rebuild()
        return


# aux values for tuple_key_to_str() and str_to_tuple_key()
_t_delim: str = chr(0)+chr(1)  # separator between key, separator between key type and key value
# map between type.__name__ and type:
_type_map: dict[str, function | type] = {'str': str, 'int': int, 'float': float, 'datetime': datetime.datetime.fromisoformat,
                                         'date': datetime.date.fromisoformat, 'time': datetime.time.fromisoformat,
                                         'Timestamp': datetime.datetime.fromisoformat, }


def tuple_key_to_str(seq: typing.Sequence) -> str:
    """ convert a sequence (or tuple) of keys to a jsonable string, usable to json
        dicts with tuple keys. Retains data types by encoding their name into the str.
        Inverse to str_to_tuple_key().
        See also #17 - perhaps Pydantic 2 will adress this. 
    """
    return _t_delim[0].join([type(s).__name__+_t_delim[1]+str(s) for s in seq])


def str_to_tuple_key(s: str) -> tuple:
    """ Inverse to tuple_key_to_str(), restores tuple """
    r = []
    for tv in s.split(_t_delim[0]):

        tn, v = tv.split(_t_delim[1], 1)
        t = _type_map.get(tn, str)
        r.append(t(v))
    return tuple(r)


def type_from_fi(fi: pydantic.fields.FieldInfo) -> type:
    """ Extract type from FieldInfo fi. 
        fi.annotation may be a generic container, get its argument. """
    t = typing.get_args(fi.annotation)
    if t:  # container, use first argument
        t = t[0]
    else:  # no container, must be class
        t = fi.annotation
    assert isinstance(t, type)
    return t
