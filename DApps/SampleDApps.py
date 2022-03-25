""" Set up a few fake Data Apps """
from __future__ import annotations
import datetime
import typing

import pydantic

from core import keys,permissions,schemas,nodes,keydirectory,principals,transactions
from core import dapp

