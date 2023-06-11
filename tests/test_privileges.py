import functools

from core import trait
from traits import privileges
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
    # non-unique classes, must be merged:
    ps = trait.Traits(p1, p2, p2a, p3, p1, p2, p3, p4)
    assert len(ps) == 3
    return


def test_merge():
    p3 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'])  # type: ignore
    p4 = privileges.OutgoingURL(urls=['https://coop.ch/dapp'])
    assert p3 is p3.merge(p3)
    pboth = privileges.OutgoingURL(urls=['https://migros.ch/dapp', 'https://coop.ch/dapp'])
    assert p3.merge(p4) == pboth

    P3 = trait.Traits(p3)
    P4 = trait.Traits(p4)
    PM = P3.merge(P4)
    assert len(PM.traits) == 1
    e = list(PM.traits)[0]
    assert e == pboth
    return


def test_cancel():
    p2 = privileges.IncomingURL(urls=['https://migros.ch/dapp'], may_overwrite=True)
    p3 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'], may_overwrite=True)  # type: ignore
    p3c = privileges.OutgoingURL(cancel=True)  # cancels p3
    ps = trait.Traits(p2, p3, p3c)
    assert len(ps) == 1  # only p2 survives
    assert privileges.IncomingURL in ps

    p4 = privileges.OutgoingURL(urls=['https://migros.ch/dapp'], may_overwrite=False)  # type: ignore
    p3c = privileges.OutgoingURL(cancel=True)  # cannot cancel p4, as this doesn't  allow overwrite
    ps = trait.Traits(p2, p4, p3c)
    assert len(ps) == 2  # only p2 and p4 survive
    assert p2 in ps
    assert p4 in ps


def test_abstract():
    p1 = privileges._DAppPrivilege()


if __name__ == '__main__':
    test_privileges()
