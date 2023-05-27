from __future__ import annotations
import pydantic
import typing


# This cannot be inline in the funtion, as it needs to be module global
WithClassVar = typing.ForwardRef('WithClassVar')  # resolved problem


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


WithClassVar.update_forward_refs()


def test_pydantic_issue_3679():
    """ Demonstrates Pydantic Bug https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    """
    wcv = WithClassVar(i=42)
    d = wcv.dict()


def test_pydantic_issue_3679_2():
    """ Demonstrates Pydantic Bug https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    """
    wcv = WithClassVar2(i=42)
    d = wcv.dict()
