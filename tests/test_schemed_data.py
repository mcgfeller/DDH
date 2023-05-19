""" Set up some Test data """


def test_get_and_putdata(user1):
    """ get data and rewrite it to same place with same user """
    r = user1.get('/ddh/mgf/org/migros.ch')
    r.raise_for_status()
    data = r.json()
    r = user1.put('/ddh/mgf/org/migros.ch', json=data)
    r.raise_for_status()
