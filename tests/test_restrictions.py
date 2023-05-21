import functools
from core import restrictions
import pytest


def test_simple_restriction():
    """ test that restriction merge properly"""
    r1 = restrictions.MustHaveSensitivites()
    r2 = restrictions.MustHaveSensitivites()
    assert r1 == r2
    assert r1.merge(r2) is r1
    r3 = restrictions.MustHaveSensitivites(may_overwrite=True)
    assert r3.merge(r1) is r1  # the non-overwriting one


def test_attributed_restriction():
    """ test that restriction merge properly"""
    r1 = restrictions.MustReview()
    r2 = restrictions.MustReview(by_roles={'boss'})
    assert r1.merge(r2) == r2
    r2a = restrictions.MustReview(may_overwrite=True, by_roles={'boss'})
    assert r1.merge(r2a) == restrictions.MustReview(may_overwrite=False, by_roles={'boss'})
    r3 = restrictions.MustReview(by_roles={'bigboss'})
    assert r2.merge(r3) == restrictions.MustReview(by_roles={'boss', 'bigboss'})


def test_restrictions():
    r1 = restrictions.MustReview()
    r2 = restrictions.MustReview(by_roles={'boss'})
    r3 = restrictions.MustHaveSensitivites()
    R1 = restrictions.Restrictions(restrictions=[r1])
    assert restrictions.MustReview in R1
    assert restrictions.MustHaveSensitivites not in R1
    R1a = restrictions.Restrictions(restrictions=[r1])
    assert R1 == R1a
    assert R1.merge(R1) is R1
    assert R1.merge(R1a) is R1

    R2 = restrictions.Restrictions(restrictions=[r2])
    assert R1.merge(R2) == restrictions.Restrictions(restrictions=[r2])  # r1 is weaker than r2
    R13 = restrictions.Restrictions(restrictions=[r1, r3])
    assert R1.merge(R13) == restrictions.Restrictions(restrictions=[r1, r3])
    assert R2.merge(R13) == restrictions.Restrictions(restrictions=[r2, r3])


def test_restrictions_overwrite():
    r1 = restrictions.MustReview(may_overwrite=True)
    r2 = restrictions.MustHaveSensitivites()
    R1 = restrictions.Restrictions(restrictions=[r1, r2])
    R2 = restrictions.Restrictions(restrictions=[~r1])
    RM = R1.merge(R2)
    assert restrictions.MustReview not in RM  # omitted by overwrite
    assert restrictions.MustHaveSensitivites in RM  # may not overwritten
