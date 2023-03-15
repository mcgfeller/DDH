import copy
import pytest
from core import keys
from utils import datautils


@pytest.fixture
def structure1():
    d = {'mgf':
         {
             'dob': 1620212,
         },

         'lise':
         {
             'a': 'bbb',
             'c': 23345.5,
         },
         }
    d['lise']['family'] = copy.deepcopy(d)
    return d


def test_extract(structure1):
    assert datautils.extract_data(structure1, keys.DDHkey('mgf/dob')) == structure1['mgf']['dob']
    assert datautils.extract_data(structure1, keys.DDHkey('lise/family/mgf/dob')) == structure1['mgf']['dob']
    assert datautils.extract_data(structure1, keys.DDHkey('lise/family/unknown'), default=None) is None
    with pytest.raises(KeyError):
        assert datautils.extract_data(structure1, keys.DDHkey('lise/family/unknown'))
    with pytest.raises(ValueError):
        assert datautils.extract_data(structure1, keys.DDHkey('lise/family/unknown'), raise_error=ValueError)
    return


def test_insert(structure1):
    s2 = datautils.insert_data(structure1, keys.DDHkey('mgf/partner'), structure1['lise'])
    assert datautils.extract_data(s2, keys.DDHkey('mgf/partner')) == datautils.extract_data(s2, keys.DDHkey('lise'))
    assert datautils.extract_data(s2, keys.DDHkey('mgf/partner/family/mgf/dob')) == structure1['mgf']['dob']
    with pytest.raises(KeyError):
        datautils.insert_data(structure1, keys.DDHkey('unknown/partner'), structure1['lise'])


def test_split(structure1):
    s2 = datautils.insert_data(structure1, keys.DDHkey('mgf/partner'), structure1['lise'])
    above, below = datautils.split_data(s2, keys.DDHkey('mgf/partner'))
    assert not datautils.extract_data(above, keys.DDHkey('mgf/partner'))
    assert below == structure1['lise']
    assert datautils.extract_data(above, keys.DDHkey('mgf/dob')) == structure1['mgf']['dob']
