""" Test combination and application of validations """

import pytest
from core import schemas, keys, trait
from traits import validations
from frontend import sessions


def test_simple_validation():
    """ test that validation merge properly"""
    r1 = validations.MustHaveSensitivites()
    r2 = validations.MustHaveSensitivites()
    assert r1 == r2
    assert r1.merge(r2) is r1
    r3 = validations.MustHaveSensitivites(may_overwrite=True)
    assert r3.merge(r1) is r1  # the non-overwriting one


def test_attributed_validation():
    """ test that validation merge properly"""
    r1 = validations.MustReview()
    r2 = validations.MustReview(by_roles={'boss'})
    assert r1.merge(r2) == r2
    r2a = validations.MustReview(may_overwrite=True, by_roles={'boss'})
    assert r1.merge(r2a) == validations.MustReview(may_overwrite=False, by_roles={'boss'})
    r3 = validations.MustReview(by_roles={'bigboss'})
    assert r2.merge(r3) == validations.MustReview(by_roles={'boss', 'bigboss'})


def test_validations():
    r1 = validations.MustReview()
    r2 = validations.MustReview(by_roles={'boss'})
    r3 = validations.MustHaveSensitivites()
    R1 = trait.Transformers(traits=[r1])
    assert validations.MustReview in R1
    assert validations.MustHaveSensitivites not in R1
    R1a = trait.Transformers(traits=[r1])
    assert R1 == R1a
    assert R1.merge(R1) is R1
    assert R1.merge(R1a) is R1

    R2 = trait.Transformers(traits=[r2])
    assert R1.merge(R2) == trait.Transformers(traits=[r2])  # r1 is weaker than r2
    R13 = trait.Transformers(traits=[r1, r3])
    assert R1.merge(R13) == trait.Transformers(traits=[r1, r3])
    assert R2.merge(R13) == trait.Transformers(traits=[r2, r3])


def test_validations_overwrite():
    r1 = validations.MustReview(may_overwrite=True)
    r2 = validations.MustHaveSensitivites()
    R1 = trait.Transformers(traits=[r1, r2])
    R2 = trait.Transformers(traits=[~r1])
    RM = R1.merge(R2)
    assert validations.MustReview not in RM  # omitted by overwrite
    assert validations.MustHaveSensitivites in RM  # may not overwritten


@pytest.mark.parametrize('ddhkey,expected', [
    ('//p/finance/holdings/portfolio', trait.DefaultTraits.RootTransformers.merge(trait.DefaultTraits.HighPrivacyTransformers)),
    ('//org/private/documents', trait.DefaultTraits.RootTransformers.merge(trait.DefaultTraits.NoValidation)),
    ('//p/living/shopping/receipts', trait.DefaultTraits.RootTransformers),
    ('//p/health/bloodworks', trait.DefaultTraits.RootTransformers.merge(trait.DefaultTraits.HighestPrivacyTransformers)),
], ids=lambda x: x if isinstance(x, str) else '')
def test_root_validations(ddhkey: str, expected: trait.Transformers, transaction):
    """ test validations in standard tree against expected results """
    schema, *d = schemas.SchemaContainer.get_node_schema_key(keys.DDHkey(ddhkey), transaction)
    transformers = schema.schema_attributes.transformers
    assert transformers == expected


@pytest.fixture(scope='module')
def transaction():
    session = sessions.get_system_session()
    return session.get_or_create_transaction()
