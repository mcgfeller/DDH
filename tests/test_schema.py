""" Test Schema manipulations """
from DApps import MigrosDApp2
import pytest




def test_schema():
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    j = MigrosDApp2.MigrosSchema.schema_json()

    return


if __name__ == '__main__':
    test_schema()