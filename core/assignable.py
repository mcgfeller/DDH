""" Qualities that can be assigned to an object """
from __future__ import annotations

import enum
import typing
import abc

import pydantic
from utils.pydantic_utils import DDHbaseModel

from . import errors


class Assignable(DDHbaseModel, typing.Hashable):
    class Config:
        frozen = True  # Assignables are not mutable, and we need a hash function to build  a set
