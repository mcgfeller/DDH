""" Demonstrates Pydantic Bug https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
"""

from __future__ import annotations
import pydantic
import typing

WithClassVar = typing.ForwardRef('WithClassVar')  # resolved problem


class WithClassVar(pydantic.BaseModel):
    # fails with issubclass() arg 1 must be a class; arg  1 is typing.ClassVar object
    Instances: typing.ClassVar[dict[str, WithClassVar]] = {}
    # Instances : typing.ClassVar[dict[str,'WithClassVar']] = {} # worked in 1.9, stopped working in 1.10.2
    # instance : dict[str,WithClassVar] = {} # no classvar - works
    i: int = 0


WithClassVar.update_forward_refs()
wcv = WithClassVar(i=42)
print(wcv)
