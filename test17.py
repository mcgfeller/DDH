""" Test Issue #17 """

from __future__ import annotations

import typing
import pydantic
from fastapi.encoders import jsonable_encoder
from core import keys
from backend import persistable

K = keys.DDHkey


class Kdict(persistable.Persistable):

    kd: dict[K, K] = {}


kd = Kdict(kd={K('k1'): K('v1')})


def test(kd):
    j = jsonable_encoder(kd)
    print(j)

    j = kd.to_json()

    kd2 = Kdict.from_json(j)
