""" Data manipulations. Manipulate dictionaries using glom.
"""
import glom
from core import keys
import copy
import typing

class _missing: 
    """ Marker class that raises error """
missing = _missing()

class _hole: 
    """ Marker class that can be returned """
hole = _hole()

def extract_data(data : dict,subkey : keys.DDHkey,default=missing,raise_error=KeyError) -> dict:
    """ extract data at subkey """
    data = glom.glom(data,subkey.key,default=default)
    if data is missing:
        raise raise_error(subkey)
    return data

def insert_data(data : dict,subkey : keys.DDHkey, newdata : dict,raise_error=KeyError, missing : typing.Callable|None = None) -> dict:
    """ insert newdata into data at subkey """
    glom.glom(data,glom.Assign('.'.join(subkey.key),newdata,missing=missing))
    return data


def split_data(data,subkey : keys.DDHkey,raise_error=KeyError) -> tuple[dict,dict]:
    """ split data so that everything in remainder is below, everything else above """
    gkey = '.'.join(subkey.key)
    below = glom.glom(data,gkey)
    above = copy.copy(data)
    glom.glom(above,glom.Assign(gkey,None))
    return above,below