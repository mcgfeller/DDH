import functools

from core import assignable
from assignables import privileges
import pytest


def test_privileges():
    """ test that privileges are hashable and form a correct set"""
    p1 = privileges.System()
    p2 = privileges.IncomingURL(urls=['https://migros.ch/dapp'])  # type: ignore # string is coerced by pydantic
    p2a = privileges.IncomingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    assert p2 == p2a, 'privileges must be identical'
    assert hash(p2) == hash(p2a), 'privileges must hash identically'
    p3 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    p4 = privileges.OutgoingURL(urls=['https://coop.ch/dapp'])
    assert len({p1, p2, p2a, p3, p1, p2, p3, p4}) == 4  # type: ignore
    with pytest.raises(ValueError):  # non-unique classes
        ps = assignable.Assignables(p1, p2, p2a, p3, p1, p2, p3, p4)
    return


def test_merge():
    p3 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    p4 = privileges.OutgoingURL(urls=['https://coop.ch/dapp'])
    assert p3 is p3.merge(p3)
    pboth = privileges.OutgoingURL(urls=['https://migros.ch/dapp', 'https://coop.ch/dapp'])
    assert p3.merge(p4) == pboth

    P3 = assignable.Assignables(p3)
    P4 = assignable.Assignables(p4)
    PM = P3.merge(P4)
    assert len(PM.assignables) == 1
    e = list(PM.assignables)[0]
    assert e == pboth
    return


def test_abstract():
    p1 = privileges._DAppPrivilege()


if __name__ == '__main__':
    test_privileges()
