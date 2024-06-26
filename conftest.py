""" Workaround to make pytest drop into VS Code debugger.
    See https://stackoverflow.com/questions/62419998/how-can-i-get-pytest-to-not-catch-exceptions/62563106#62563106.
"""
import os
import pytest
import sys
import pathlib

os.environ['RAISE_IMPORT_ERRORS'] = 'True'

d = str(pathlib.Path(__file__).parent)  # project dir
if d not in sys.path:
    sys.path.insert(0, d)


if os.getenv('_PYTEST_RAISE', '') == "1":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

from tests.service_fixtures import *  # import all fixtures for services (fixtures have to be declared globally)
from tests.schema_fixtures import *  # import all fixtures for schema testing (fixtures have to be declared globally)
