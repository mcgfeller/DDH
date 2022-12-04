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
