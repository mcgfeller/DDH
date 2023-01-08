""" Tests for dynamic manipulation of Pydantic classes """

from __future__ import annotations
import pydantic
import typing
import datetime
from utils.pydantic_utils import DDHbaseModel


class LowerClass(DDHbaseModel):
    """ Details of a product """
    produkt_kategorie: str
    garantie: str | None = None
    garantie_jahre: int | None = 1
    beschreibung: str = ''
    labels: list[str] = []


class UpperClass(DDHbaseModel):

    """ The Receipt of an individual purchase """

    Datum_Zeit: datetime.datetime
    Filiale:    str
    Kassennummer:  int
    Transaktionsnummer: int
    Artikel:    str
    Menge:      float = 1
    Aktion:     int = 0
    Umsatz:     float = 0
    Produkt: LowerClass | None = None


class TopClass(DDHbaseModel):
    """ A fake Migros schema, showing Cumulus receipts """
    cumulus: int | None = None
    receipts: list[UpperClass] = []


def test_add_simple():
    """ Apparently simple fields cannot be added to a Pydantic class """
    TopClass.add_fields(cumulus=(typing.Optional[int], None))
    assert 'cumulus' in TopClass.__fields__
    check(TopClass)
    return


def test_add_subclass():
    TopClass.add_fields(Produkt2=(LowerClass, None))
    check(TopClass)


def test_add_deep_subclass():
    TopClass.__fields__['receipts'].type_.add_fields(last_receipts=(UpperClass, None))
    TopClass.__fields__['receipts'].type_.add_fields(last_receipts=(UpperClass, None), last_product=(LowerClass, None))
    check(TopClass)


def test_add_empty_subclass():
    empty = pydantic.create_model('empty', __base__=DDHbaseModel)
    empty2 = pydantic.create_model('empty2', __base__=DDHbaseModel)
    TopClass.__fields__['receipts'].type_.add_fields(empty=(empty, None))
    empty.add_fields(empty2=(empty2, None))
    empty2.add_fields(last_receipts=(UpperClass, None))
    check(empty)
    check(TopClass)


def check(cls):
    cls.schema()
    a = list(cls.__fields__.items())
