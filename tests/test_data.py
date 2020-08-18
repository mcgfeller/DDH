""" Set up some Test data """
import core
from . import MigrosDapp

owner = core.User(id='mgf',name='Martin')
k1 = core.DDHkey(key="/ddh/shopping/stores/migros/receipts/mgf")

print(k1)

