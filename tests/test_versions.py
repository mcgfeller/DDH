import functools
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

def upgrade(v_from,v_to):
    print(f'Upgrading {v_from} from to {v_to}')
    return

def test_upgrades():
    up = versions.Upgraders()
    for t_from,t_to in (('1.0','1.1'),('1.1','1.2'),('1.2','1.3'),('1.1','1.3'),('1.3','2.0')):
        v_from = versions.Version(t_from)
        v_to = versions.Version(t_to)
        upf = functools.partial(upgrade,v_from,v_to)
        up.add_upgrader(v_from,v_to,functools.partial(upgrade,v_from,v_to))
        assert up.upgrade_path(v_from,v_to)[0].args == (v_from,v_to)
    assert 3 == len(up.upgrade_path(versions.Version('1.0'),versions.Version('2.0'))) # 1.0 -> 1.1 -> 1.3 -> 2.0
    assert 2 == len(up.upgrade_path(versions.Version('1.0'),versions.Version('1.3'))) # 1.0 -> 1.1 -> 1.3
    assert 2 == len(up.upgrade_path(versions.Version('1.2'),versions.Version('2.0'))) # 1.2 -> 1.3 -> 2.0

if __name__ == '__main__':
    test_version_string()