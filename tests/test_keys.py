from core import keys
import pytest


def test_paths():
    ddhkey1 = keys.DDHkey(key='norooted')
    ddhkey2 = keys.DDHkey(key='norooted/subkey')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = keys.DDHkey(key='/rooted')
    ddhkey4 = keys.DDHkey(key='/rooted/subkey')
    assert ddhkey3 == ddhkey4.up()
    assert ddhkey3.up().up() is None
    ddhkey = keys.DDHkey(key=())

    return


def test_forks():
    ddhkey1 = keys.DDHkey(key='norooted:schema')
    ddhkey2 = keys.DDHkey(key='norooted/subkey:schema')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = keys.DDHkey(key='norooted/subkey')
    assert ddhkey1 != ddhkey3.up()  # forks don't match

    ddhkey4 = keys.DDHkey(key='norooted/subkey:data')
    assert ddhkey3 == ddhkey4

    with pytest.raises(ValueError):
        keys.DDHkey(key='norooted/subkey:stupid')  # invalid key


def test_variant():
    ddhkey1 = keys.DDHkey(key='norooted:data:recommended')
    ddhkey2 = keys.DDHkey(key='norooted/subkey:data:recommended')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = keys.DDHkey(key='norooted/subkey:data:rec1')
    assert ddhkey1 != ddhkey3.up()  # variants don't match

    ddhkey4 = keys.DDHkey(key='norooted/subkey:data:rec1')
    assert ddhkey3 == ddhkey4

    ddhkey5 = keys.DDHkey(key='norooted/subkey::rec1')  # omit fork
    assert ddhkey3 == ddhkey4



def test_version():
    ddhkey1 = keys.DDHkey(key='norooted:data::unspecified')
    ddhkey2 = keys.DDHkey(key='norooted/subkey:data::unspecified')
    assert ddhkey1 == ddhkey2.up()

    ddhkey1a = keys.DDHkey(key='norooted:data::unspecified')
    ddhkey1b = keys.DDHkey(key='norooted:::unspecified')
    ddhkey1c = keys.DDHkey(key='norooted:::unspecified')
    assert ddhkey1 == ddhkey1a
    assert ddhkey1 == ddhkey1b
    assert ddhkey1 == ddhkey1c

    ddhkey3 = keys.DDHkey(key='norooted/subkey:::4.0')
    assert ddhkey1 != ddhkey3.up()  # versions don't match

    ddhkey4 = keys.DDHkey(key='norooted/subkey:data::5.0')
    assert ddhkey3 != ddhkey4

    with pytest.raises(ValueError):
        keys.DDHkey(key='norooted/subkey:::4.x')  # invalid version

    d = str(ddhkey1),str(ddhkey1a),str(ddhkey1b),str(ddhkey1c),str(ddhkey2),str(ddhkey3),str(ddhkey4)
