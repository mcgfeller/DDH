
import pydantic
import typing
from fastapi.encoders import jsonable_encoder
import json
from core import keys

# This cannot be inline in the funtion, as it needs to be module global
# WithClassVar = typing.ForwardRef('WithClassVar')  # resolved problem in Pyd1, no longer required in Pyd2


class WithClassVar(pydantic.BaseModel):
    # fails with issubclass() arg 1 must be a class; arg  1 is typing.ClassVar object
    Instances: typing.ClassVar[dict[str, WithClassVar]] = {}
    # instance : dict[str,WithClassVar] = {} # no classvar - works
    i: int = 0


class WithClassVar2(pydantic.BaseModel):
    # fails with issubclass() arg 1 must be a class; arg  1 is typing.ClassVar object
    # worked in 1.9, stopped working in 1.10.2, works again in 1.10.8
    Instances: typing.ClassVar[dict[str, 'WithClassVar']] = {}
    i: int = 0


WithClassVar.model_rebuild()


def test_pydantic_issue_3679():
    """ Demonstrates Pydantic Bug https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    """
    wcv = WithClassVar(i=42)
    d = wcv.model_dump()


def test_pydantic_issue_3679_2():
    """ Demonstrates Pydantic Bug https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    """
    wcv = WithClassVar2(i=42)
    d = wcv.model_dump()


class Simple(pydantic.BaseModel):

    ext_ref: typing.ClassVar[pydantic.AnyUrl] = pydantic.AnyUrl('https://example.com/schema')
    # #42: String key won't work, key won't be stringed by jsonable_encoder (as it is a model):
    ext_ref_k: typing.ClassVar[keys.DDHkey] = keys.DDHkey('//org/swisscom.com')

    @staticmethod
    def _json_schema_extra(schema: dict[str, typing.Any], model: typing.Type[Simple]) -> None:
        """ Generate  JSON Schema as a reference to the URI.

            NOTE #42: As Pydantic 2 can only include objects that are not instances of str as $ref
        """
        schema['properties']['dep'] = {'$ref': model.ext_ref}
        return
    model_config = pydantic.ConfigDict(json_schema_extra=_json_schema_extra)


def test_pydantic_schema_ref():
    j = Simple.model_json_schema()
    s = json.dumps(jsonable_encoder(j))
    assert str(Simple.ext_ref) in s
    return
