""" Tests with Schemas and Data from DApps, working with all microservices """


def test_get_data(user1):
    r = user1.get('/ddh/mgf/org/migros.ch/receipts')
    r.raise_for_status()
    data = r.json()
    assert isinstance(data, dict)
    assert len(data) > 0
    assert isinstance(data['mgf'], list)
    assert len(data['mgf']) > 10
    assert all(a in data['mgf'][5]
               for a in ('Datum_Zeit', 'Menge', 'Filiale'))  # these keys must be present
    assert r.headers['content-location'] == str(user1.base_url)+'/ddh/mgf/org/migros.ch/receipts::PySchema:0'
    return


def test_get_data_wrong_mimetype(user1):
    r = user1.get('/ddh/mgf/org/migros.ch/receipts', headers={'Accept': 'application/xml'})
    assert 406 == r.status_code
    msg = r.json()['detail']
    assert msg
    return


def test_dapp_read_data_no_owner(user1):
    """ test retrieval of no-owner key of test MigrosDApp """
    r = user1.get('/ddh//org/migros.ch/receipts')
    assert 404 == r.status_code
    assert r.json()['detail'] == 'key has no owner'
    return


def test_dapp_read_data_unknown(user1):
    """ test retrieval of key of test MigrosDApp, with a user that does not exist """
    r = user1.get('/ddh/mgf,unknown/org/migros.ch/receipts')
    assert 404 == r.status_code
    assert r.json()['detail'] == 'User not found unknown'
    return


def test_dapp_read_data_nopermit(user1):
    """ test retrieval of key of test MigrosDApp, with a user that has no permission """
    r = user1.get('/ddh/another/org/migros.ch/receipts')
    assert 403 == r.status_code
    return


def test_std_read_data(user1):
    """ test retrieval of key of test MigrosDApp with transformation to standard """
    r = user1.get('/ddh/mgf/p/living/shopping/receipts')
    r.raise_for_status()
    data = r.json()
    assert isinstance(data, dict)
    assert len(data) > 0
    assert isinstance(data['items'], list)
    assert len(data['items']) > 10
    assert all(a in data['items'][5]
               for a in ('article', 'quantity', 'buyer'))  # these keys must be present

    return


def test_dapp_schema(user1):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    r = user1.get('/ddh//org/migros.ch/receipts:schema')
    r.raise_for_status()
    d = r.json()
    assert isinstance(d, dict)
    assert d
    assert d['title'] == 'Receipt'  # type: ignore
    return


def test_dapp_schema_2(user1):
    """ test retrieval of key of test MigrosDApp, and facade.get_schema() """
    r = user1.get('/ddh//org/migros.ch/receipts/Produkt:schema')
    r.raise_for_status()
    d = r.json()
    assert d['title'] == 'ProduktDetail'  # type: ignore
    return


def test_complete_schema_p(user1):
    r = user1.get('/ddh//org:schema')
    r.raise_for_status()
    d = r.json()
    assert d.get('title'), 'schema is empty'
    return


def test_p_schema(user1):
    r = user1.get('/ddh//p/living/shopping:schema')
    r.raise_for_status()
    d = r.json()
    assert d.get('title'), 'schema is empty'
    return


def test_graph_from(user1):
    r = user1.get('/graph/from/MigrosDApp+CoopDApp')
    r.raise_for_status()
    d = r.json()
    assert len(d) == 2, 'one result per app'
    return  # TODO: Check result


def test_graph_to(user1):
    r = user1.get('/graph/from/TaxCalc')
    r.raise_for_status()
    d = r.json()
    assert len(d) == 1, 'one app, one result'
    assert 'SBBempDApp' in d[0]  # TODO: Check result
    assert 'UBSaccount' in d[0]
    return


def test_schema_graph(user1):
    r = user1.get('/graph/draw?layout=shell_layout&size_h=1000')
    r.raise_for_status()
    assert r.headers['content-type'] == 'image/png'
    return


def test_schema_ego_graph(user1):
    r = user1.get('/graph/draw?layout=planar_layout&size_h=1000&center_schema=//p/employment/salary/statements&radius=4')
    r.raise_for_status()
    assert r.headers['content-type'] == 'image/png'
    return
