""" Test combination and application of validations """

import pytest
from core import trait, permissions, keys
from traits import validations, capabilities, anonymization


def test_orderings():
    """ test Transformes sort  """
    t1 = trait.Transformers(validations.MustHaveSensitivites(),
                            validations.LatestVersion(), anonymization.Pseudonymize(), validations.MustValidate())
    s1r = t1.sorted(t1.traits, {permissions.AccessMode.read})
    s1w = t1.sorted(t1.traits, {permissions.AccessMode.write})
    assert len(t1) == len(s1w)
    return


def test_orderings_after():
    """ test Transformes sort, with TestTransfomer with .after specification  """
    class TestTransformer(capabilities.DataCapability):
        after = 'LatestVersion'

    t2 = trait.Transformers(TestTransformer(), validations.MustHaveSensitivites(), validations.LatestVersion(),
                            anonymization.Pseudonymize(), validations.MustValidate())
    s2w = t2.sorted(t2.traits, {permissions.AccessMode.write})
    assert len(t2) == len(s2w)
    assert s2w[-1].classname == 'TestTransformer'
    return


def test_select():
    """ test that Transformers are selected """
    t1 = trait.Transformers(validations.MustHaveSensitivites(),
                            validations.LatestVersion(), anonymization.Pseudonymize(), anonymization.DePseudonymize(), validations.MustValidate())
    s1r = t1.select_for_apply({permissions.AccessMode.read, permissions.AccessMode.pseudonym}, keys.ForkType.data)
    assert len(s1r) == 1
    s1w = t1.select_for_apply({permissions.AccessMode.write, permissions.AccessMode.pseudonym}, keys.ForkType.data)
    assert len(s1w) == 3
    s1sw = t1.select_for_apply({permissions.AccessMode.write, permissions.AccessMode.pseudonym}, keys.ForkType.schema)
    assert len(s1sw) == 1
    return


def test_add():
    t1 = trait.Transformers(validations.MustHaveSensitivites(),
                            validations.LatestVersion(), validations.MustValidate())
    t2 = trait.Transformers(validations.LatestVersion(), anonymization.Pseudonymize())
    t1 += t2
    assert len(t1) == 4
    assert validations.LatestVersion in t1
    assert anonymization.Pseudonymize in t1
    assert validations.MustValidate in t1
