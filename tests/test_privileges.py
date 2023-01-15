import functools
from core import privileges
import pytest


def test_privileges():
    """ test that privileges are hashable and form a correct set"""
    p1 = privileges.System()
    p2 = privileges.IncomingURL(urls=['https://migros.ch/dapp'])  # type: ignore # string is coerced by pydantic
    p2a = privileges.IncomingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    assert p2 == p2a, 'privileges must be identical'
    assert hash(p2) == hash(p2a), 'privileges must hash identically'
    p3 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    p4 = privileges.OutgoingURL(urls=['https://coop.ch/dapp'])  # type: ignore
    assert len({p1, p2, p2a, p3, p1, p2, p3, p4}) == 4  # type: ignore # no idea why it thinks px is not hashable?


def test_abstract():
    p1 = privileges._DAppPrivilege()


if __name__ == '__main__':
    test_privileges()
