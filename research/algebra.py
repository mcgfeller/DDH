import typing
from core import pillars
from core import keys,permissions,schemas,nodes,errors


def transform_plain(k : keys.DDHkey): ...

def transform_encrypted(k : keys.DDHkey): ...

def transform_under_decrypt(k : keys.DDHkey): ...

