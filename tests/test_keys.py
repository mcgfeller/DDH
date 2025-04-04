from core import keys, versions
import pytest


def test_paths():
    ddhkey1 = keys.DDHkey(key='norooted')
    ddhkey1n = keys.DDHkey('norooted')
    assert ddhkey1 == ddhkey1n
    ddhkey2 = keys.DDHkey(key='norooted/subkey')
    assert ddhkey1 == ddhkey2.up()

    ddhkey3 = keys.DDHkey(key='/rooted')
    ddhkey4 = keys.DDHkey(key='/rooted/subkey')
    assert ddhkey3 == ddhkey4.up()
    assert not ddhkey3.up().up()
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


def test_multiowner():
    """ Keys with more than one owner """
    assert ',' == keys.DDHkey.OwnerDelimiter
    ddhkey1 = keys.DDHkey(key='/u1,u2/subkey')
    assert ddhkey1.owner == 'u1,u2'
    assert ddhkey1.owners == ('u1', 'u2')

    ddhkey2 = keys.DDHkey(key='//subkey')
    assert ddhkey2.owners == ()
    return


def test_variant():
    ddhkey1 = keys.DDHkey(key='norooted:data:recommended')
    ddhkey2 = keys.DDHkey(key='norooted/subkey:data:recommended')
    assert ddhkey1 == ddhkey2.up(retain_specifiers=True)
    assert ddhkey1 != ddhkey2.up()  # variant does not match, as it is not retained

    ddhkey3 = keys.DDHkey(key='norooted/subkey:data:rec1')
    assert ddhkey1 != ddhkey3.up(retain_specifiers=True)  # variants don't match

    ddhkey4 = keys.DDHkey(key='norooted/subkey:data:rec1')
    assert ddhkey3 == ddhkey4

    ddhkey5 = keys.DDHkey(key='norooted/subkey::rec1')  # omit fork
    assert ddhkey3 == ddhkey4


def test_version():
    ddhkey1 = keys.DDHkey(key='norooted:data::unspecified')
    assert str(ddhkey1) == 'norooted'
    ddhkey2 = keys.DDHkey(key='norooted/subkey:data::unspecified')
    assert str(ddhkey2) == 'norooted/subkey'
    assert ddhkey1 == ddhkey2.up()

    ddhkey1v = keys.DDHkey(key='norooted/subkey:schema:spec:1.0')
    assert str(ddhkey1v) == 'norooted/subkey:schema:spec:1.0'

    ddhkey1a = keys.DDHkey(key='norooted:data::unspecified')
    ddhkey1b = keys.DDHkey(key='norooted:::unspecified')
    ddhkey1c = keys.DDHkey(key='norooted:::unspecified')
    assert ddhkey1 == ddhkey1a
    assert ddhkey1 == ddhkey1b
    assert ddhkey1 == ddhkey1c

    ddhkey3 = keys.DDHkey(key='norooted/subkey:::4.0')
    assert str(ddhkey3) == 'norooted/subkey:::4.0'
    assert ddhkey1 == ddhkey3.up()  # version not retained, matches
    assert ddhkey1 != ddhkey3.up(retain_specifiers=True)  # versions don't match

    ddhkey4 = keys.DDHkey(key='norooted/subkey:data::5.0')
    assert str(ddhkey4) == 'norooted/subkey:::5.0'
    assert ddhkey3 != ddhkey4

    with pytest.raises(ValueError):
        keys.DDHkey(key='norooted/subkey:::4.x')  # invalid version


def test_without_variant_version():
    ddhkey1 = keys.DDHkey(key='norooted/subkey:schema:spec:1.0')
    ddh_wvv = ddhkey1.without_variant_version()
    assert ddh_wvv.fork == keys.ForkType.schema
    assert ddh_wvv.variant == keys.DefaultVariant
    assert ddh_wvv.version == keys.versions.Unspecified


def test_split():
    """ Test key split """
    ddhkey1 = keys.DDHkey("/mgf/p/living/shopping/receipts::PySchema")
    k0, k1 = ddhkey1.split_at(5)
    assert str(k0) == "/mgf/p/living/shopping::PySchema"
    assert str(k1) == "receipts"


def test_range():
    ddhkey1 = keys.DDHkeyRange(key="//org/ubs.com/switzerland/customer/account:::>0")
    assert versions.Version(0) not in ddhkey1.version
    assert versions.Version(1) in ddhkey1.version


def test_new_owner():
    ddhkey1 = keys.DDHkey("/mgf/p/living/shopping/receipts")
    assert ddhkey1.with_new_owner('lise') == keys.DDHkey("/lise/p/living/shopping/receipts")
    ddhkey2 = keys.DDHkey("//p/living/shopping/receipts")
    assert ddhkey1.with_new_owner('mgf') == keys.DDHkey("/mgf/p/living/shopping/receipts")


test_keys = [
    keys.DDHkey('norooted'),
    keys.DDHkey('norooted/subkey'),
    keys.DDHkey(key='/u1,u2/subkey'),
    keys.DDHkey("/mgf/p/living/shopping/receipts"),
    keys.DDHkey("/mgf/p/living/shopping/receipts::PySchema"),
    keys.DDHkey(key='norooted/subkey:schema:spec:1.0'),
    keys.DDHkeyGeneric("/mgf/p/living/shopping/receipts"),
    keys.DDHkeyVersioned(key='norooted/subkey:schema:spec:1.0'),
    keys.DDHkeyRange(key="//org/ubs.com/switzerland/customer/account:::>1"),
]

# id must not contain ':'!
all_test_keys = pytest.mark.parametrize('key', test_keys, ids=lambda key: str(key).replace(':', '_'))


@all_test_keys
def test_roundtrip_dict(key):
    cls = key.__class__
    d = key.model_dump()
    rkey = cls.model_validate(d)
    assert key == rkey, 'mismatch: key -> dict -> validate == key'


@all_test_keys
def test_roundtrip_json(key):
    cls = key.__class__
    j = key.model_dump_json()
    rkey = cls.model_validate_json(j)
    assert key == rkey, 'mismatch: key -> json -> validate_json  == key'


@all_test_keys
def test_roundtrip_str(key):
    cls = key.__class__
    s = str(key)
    rkey = cls(s)
    assert key == rkey, 'mismatch: key -> str -> cls()  == key'
