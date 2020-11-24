""" DDH abstract storage
"""
from __future__ import annotations
from abc import abstractmethod
import typing

from core import keys,permissions,nodes
from utils.pydantic_utils import NoCopyBaseModel


class StorageNode(nodes.ExecutableNode):
    """ node with storage on DDH """
    ...