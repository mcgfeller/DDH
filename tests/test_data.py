""" Set up some Test data """
import core

owner = core.User(id='mgf',name='Martin')
k1 = core.DDHkey(key="ddh/shopping/stores/migros/receipts/mgf",owner=owner)

print(k1)

