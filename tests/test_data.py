""" Set up some Test data """
import model

owner = model.User(id='mgf',name='Martin')
k1 = model.DDHkey(key="ddh/shopping/stores/migros/receipts/mgf",owner=owner)

print(k1)

