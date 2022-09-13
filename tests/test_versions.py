import functools
from core import versions
import pytest

def test_version_string():
    assert versions.Version('1.5.6').dotted() == '1.5.6'
    assert versions.Version('1.5.6.2').dotted() == '1.5.6.2'
    assert versions.Version('1.5.0.2.3').dotted() == '1.5.0.2'

    assert versions.Version('6.6.6',alias='the beast').dotted() == '6.6.6 [the beast]'
    return

def test_version_bad():
    with pytest.raises(ValueError):
        versions.Version('1.A.6')
    with pytest.raises(ValueError):
        versions.Version('1..6')

    with pytest.raises(ValueError):
        versions.Version('') # empty


def test_version_tuple():
    assert versions.Version(1,5,6).dotted() == '1.5.6'
    assert versions.Version(1,5,0).dotted() == '1.5.0'
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
    assert versions.Version('4.0') in versions.VersionConstraint('==4.0')
    assert versions.Version('4.0') in versions.VersionConstraint('>=4.0')
    assert versions.Version('4.5') in versions.VersionConstraint('>=4.0,<=5.0')
    assert versions.Version('4.0') in versions.VersionConstraint('>=4.0,<5.0')
    assert versions.Version('5.0') not in versions.VersionConstraint('>=4.0,<5.0')
    assert versions.Version('4.0') not in versions.VersionConstraint('>4.0,<5')
    assert versions.VersionConstraint('>4.0,<5') == versions.VersionConstraint('<5,>4.0') # rearrange

def test_version_noconstraint():
    assert versions.Version('4.0') in versions.NoConstraint
    assert versions.Unspecified in versions.NoConstraint


def test_version_constraint_bad():
    with pytest.raises(ValueError):
        versions.VersionConstraint('4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('!=4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<=>4.0')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<4.0,>5,>6')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<4.0,<6')
    with pytest.raises(ValueError):
        versions.VersionConstraint('>4.0,>6')
    with pytest.raises(ValueError):
        versions.VersionConstraint('<4.0,>5')

def test_comparisons():
    v = versions.Version('1.5.6')
    assert v in versions.VersionConstraint('==1.5.6')
    assert v in versions.VersionConstraint('<=1.5.6')
    assert v not in versions.VersionConstraint('<1.5.6')
    assert v not in versions.VersionConstraint('>1.6')
    assert v not in versions.VersionConstraint('>1.6')
    assert v in versions.VersionConstraint('>1.5,<1.6')
    assert v not in versions.VersionConstraint('>1.5,<1.5.5')

def upgrade(v_from,v_to,*a,**kw):
    print(f'Upgrading {v_from} from to {v_to}')
    return True

def test_upgrades():
    up = versions.Upgraders()
    for t_from,t_to in (('1.0','1.1'),('1.1','1.2'),('1.2','1.3'),('1.1','1.3'),('1.3','2.0'),('2.1','3.0')):
        v_from = versions.Version(t_from)
        v_to = versions.Version(t_to)
        up.add_upgrader(v_from,v_to,upgrade)
        assert up.upgrade_path(v_from,v_to)[0] == upgrade
    assert 3 == len(up.upgrade_path(versions.Version('1.0'),versions.Version('2.0'))) # 1.0 -> 1.1 -> 1.3 -> 2.0
    assert 2 == len(up.upgrade_path(versions.Version('1.0'),versions.Version('1.3'))) # 1.0 -> 1.1 -> 1.3
    assert 2 == len(up.upgrade_path(versions.Version('1.2'),versions.Version('2.0'))) # 1.2 -> 1.3 -> 2.0
    with pytest.raises(ValueError):
        up.upgrade_path(versions.Version('1.2'),versions.Version('2.2')) # 2.2 not known
    with pytest.raises(ValueError):
        up.upgrade_path(versions.Version('1.2'),versions.Version('3.0')) # no path between 2.0 and 2.1

if __name__ == '__main__':
    test_version_string()