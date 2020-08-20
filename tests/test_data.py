""" Set up some Test data """
import core
from . import MigrosDapp

owner = core.User(id='mgf',name='Martin')
k1 = core.DDHkey(key="/ddh/shopping/stores/migros/receipts/mgf")

def test_dapp():
    mdapp = MigrosDapp.MigrosDApp(owner=owner,schemakey=k1)
    mdapp.startup()
    return


test_dapp()