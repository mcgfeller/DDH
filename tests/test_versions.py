from core import versions
import pytest

def test_version_string():
    assert versions.Version('1.5.6').dotted() == '1.5.6.0'
    assert versions.Version('1.5.6.2').dotted() == '1.5.6.2'
    assert versions.Version('1.5.0.2.3').dotted() == '1.5.0.2'

    assert versions.Version('6.6.6',alias='the beast').dotted() == '6.6.6.0 [the beast]'
    return

def test_version_bad():
    with pytest.raises(ValueError):
        versions.Version('1.A.6')
    with pytest.raises(ValueError):
        versions.Version('1..6')

    with pytest.raises(ValueError):
        versions.Version('') # empty


def test_version_tuple():
    assert versions.Version(1,5,6).dotted() == '1.5.6.0'
    assert versions.Version(1,5,0).dotted() == '1.5.0.0'
    return


def test_version_compare():
    v0 = versions.Version('1.5.6')
    assert v0 == versions.Version(1,5,6)
    assert v0 != versions.Version(1,5,7)
    assert v0 <  versions.Version(1,5,7)
    assert not v0 >  versions.Version(1,5,6)
    assert v0 >=  versions.Version(1,5,6)
    assert v0 <=  versions.Version(1,5,7)
    assert v0 <=  versions.Version(1,5,6)

def test_version_constraint():
    versions.VersionConstraint('==4.0')
    versions.VersionConstraint('>=4.0')
    versions.VersionConstraint('>=4.0,<=5.0')
    versions.VersionConstraint('>=4.0,<=5.0')
    versions.VersionConstraint('<4.0,>5')

def test_version_constraint_bad():
    with pytest.raises(ValueError):
        versions.VersionConstraint('4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('!=4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<=>4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<4.0,>5,>6')

def test_comparisons():
    v = versions.Version('1.5.6')
    assert v in versions.VersionConstraint('==1.5.6')
    assert v in versions.VersionConstraint('<=1.5.6')
    assert v not in versions.VersionConstraint('<1.5.6')
    assert v not in versions.VersionConstraint('>1.6')
    assert v not in versions.VersionConstraint('>1.6')
    assert v in versions.VersionConstraint('>1.5,<1.6')
    assert v not in versions.VersionConstraint('>1.5,<1.5.5')

if __name__ == '__main__':
    test_version_string()