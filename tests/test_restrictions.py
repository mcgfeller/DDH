""" Test combination and application of restrictions """

import pytest
from core import restrictions, schemas, keys
from assignables import schema_restrictions
from frontend import sessions


def test_simple_restriction():
    """ test that restriction merge properly"""
    r1 = schema_restrictions.MustHaveSensitivites()
    r2 = schema_restrictions.MustHaveSensitivites()
    assert r1 == r2
    assert r1.merge(r2) is r1
    r3 = schema_restrictions.MustHaveSensitivites(may_overwrite=True)
    assert r3.merge(r1) is r1  # the non-overwriting one


def test_attributed_restriction():
    """ test that restriction merge properly"""
    r1 = schema_restrictions.MustReview()
    r2 = schema_restrictions.MustReview(by_roles={'boss'})
    assert r1.merge(r2) == r2
    r2a = schema_restrictions.MustReview(may_overwrite=True, by_roles={'boss'})
    assert r1.merge(r2a) == schema_restrictions.MustReview(may_overwrite=False, by_roles={'boss'})
    r3 = schema_restrictions.MustReview(by_roles={'bigboss'})
    assert r2.merge(r3) == schema_restrictions.MustReview(by_roles={'boss', 'bigboss'})


def test_restrictions():
    r1 = schema_restrictions.MustReview()
    r2 = schema_restrictions.MustReview(by_roles={'boss'})
    r3 = schema_restrictions.MustHaveSensitivites()
    R1 = restrictions.Restrictions(assignables=[r1])
    assert schema_restrictions.MustReview in R1
    assert schema_restrictions.MustHaveSensitivites not in R1
    R1a = restrictions.Restrictions(assignables=[r1])
    assert R1 == R1a
    assert R1.merge(R1) is R1
    assert R1.merge(R1a) is R1

    R2 = restrictions.Restrictions(assignables=[r2])
    assert R1.merge(R2) == restrictions.Restrictions(assignables=[r2])  # r1 is weaker than r2
    R13 = restrictions.Restrictions(assignables=[r1, r3])
    assert R1.merge(R13) == restrictions.Restrictions(assignables=[r1, r3])
    assert R2.merge(R13) == restrictions.Restrictions(assignables=[r2, r3])


def test_restrictions_overwrite():
    r1 = schema_restrictions.MustReview(may_overwrite=True)
    r2 = schema_restrictions.MustHaveSensitivites()
    R1 = restrictions.Restrictions(assignables=[r1, r2])
    R2 = restrictions.Restrictions(assignables=[~r1])
    RM = R1.merge(R2)
    assert schema_restrictions.MustReview not in RM  # omitted by overwrite
    assert schema_restrictions.MustHaveSensitivites in RM  # may not overwritten


@pytest.mark.parametrize('ddhkey,expected', [
    ('//p/finance/holdings/portfolio', schema_restrictions.HighPrivacyRestrictions),
    ('//org/private/documents', schema_restrictions.NoRestrictions),
    ('//p/living/shopping/receipts', schema_restrictions.RootRestrictions),
    ('//p/health/bloodworks', schema_restrictions.HighestPrivacyRestrictions),
], ids=lambda x: x if isinstance(x, str) else '')
def test_root_restrictions(ddhkey: str, expected: restrictions.Restrictions, transaction):
    """ test restrictions in standard tree against expected results """
    schema, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(ddhkey), transaction)
    restrictions = schema.schema_attributes.restrictions
    assert restrictions == expected


@pytest.fixture(scope='module')
def transaction():
    session = sessions.get_system_session()
    return session.get_or_create_transaction()
