
import os, os.path
from utils import utils


def listAllSubPackages(rootpackage, omitDirs=None):
    """ return as list all Python modules in rootpackage """
    dir = os.path.dirname(rootpackage.__file__)  # directory where rootpackage is located
    ldir = len(dir)+1
    modulenames = []
    suffixes = ('.py',)  # only Python files
    for (dir, dirs, fns) in os.walk(dir):
        if omitDirs:
            [dirs.remove(omit) for omit in omitDirs if omit in dirs]  # must remove in-place
        for fn in fns:
            sfn = os.path.splitext(os.path.join(dir, fn))
            if sfn[1] in suffixes:
                modulename = sfn[0][ldir:]
                modulename = modulename.replace(os.path.sep, '.')
                modulenames.append(rootpackage.__name__+'.'+modulename)
    return modulenames


def importAllSubPackages(rootpackage, omitInits=True, omitSuffix=None, omitPrefix='test_', omitDirs=('tests',), raiseError=False):
    """ import all Python modules below rootpackage module
        return list of module objects.

        omitInits avoids explicit loading of __init__ modules, as they are loaded implicitely.  
    """
    modulenames = []
    for path in listAllSubPackages(rootpackage, omitDirs=omitDirs):
        m = path.split('.')[-1]  # modul
        if m.startswith(omitPrefix) or omitInits and m.endswith('__init__') or omitSuffix and m.endswith(omitSuffix):
            continue
        modulenames.append(path)
    return importModules(modulenames, raiseError=raiseError)


def importModules(modulenames, raiseError=False):
    """ import list of named modules
        if raiseError is True, any error is raised, otherwise errors are logged.
    """
    modules = []
    for modulename in modulenames:
        module = utils.load_module(modulename, raiseError=raiseError)
        if module:
            modules.append(module)
    return modules
