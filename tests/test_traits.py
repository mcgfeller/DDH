""" Test combination and application of restrictions """

import pytest
from core import trait, permissions
from traits import restrictions, capabilities, anonymization


def test_orderings():
    """ test Transformes sort  """
    t1 = trait.Transformers(restrictions.MustHaveSensitivites(),
                            restrictions.LatestVersion(), anonymization.Pseudonymize(), restrictions.MustValidate())
    s1r = t1.sorted(t1.traits, {permissions.AccessMode.read})
    s1w = t1.sorted(t1.traits, {permissions.AccessMode.write})
    assert len(t1) == len(s1w)
    return


def test_orderings_after():
    """ test Transformes sort, with TestTransfomer with .after specification  """
    class TestTransformer(capabilities.SchemaCapability):
        after = 'LatestVersion'

    t2 = trait.Transformers(TestTransformer(), restrictions.MustHaveSensitivites(), restrictions.LatestVersion(),
                            anonymization.Pseudonymize(), restrictions.MustValidate())
    s2w = t2.sorted(t2.traits, {permissions.AccessMode.write})
    assert len(t2) == len(s2w)
    assert s2w[-1].classname == 'TestTransformer'
    return


def test_select():
    """ test that Transformers are selected """
    t1 = trait.Transformers(restrictions.MustHaveSensitivites(),
                            restrictions.LatestVersion(), anonymization.Pseudonymize(), restrictions.MustValidate())
    s1r = t1.select_for_apply({permissions.AccessMode.read, permissions.AccessMode.pseudonym})
    assert len(s1r) == 1
    s1w = t1.select_for_apply({permissions.AccessMode.write, permissions.AccessMode.pseudonym})
    assert len(s1w) == 4
    return
