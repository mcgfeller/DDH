

import glom
import datetime
from pandas import Timestamp
from pprint import pprint
from glom import  T, A, S, Merge, Iter, Coalesce, Val

target = {
    "pluto": {"moons": 6, "population": None},
    "venus": {"population": {"aliens": 5}},
    "earth": {"moons": 1, "population": {"humans": 7700000000, "aliens": 1}},
 }
spec = {
     "moons": (
          T.items(),
          Iter({T[0]: (T[1], Coalesce("moons", default=0))}),
          Merge(),
     )
 }
pprint(glom.glom(target, spec))

target2 =  {'mgf': {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Kopfsalat mit Herz', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 2.6},
        'lise':  {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Laugentessinerbrot', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 2.65},
            }

spec2 = {
     "items": (
          T.items(),
          Iter({T[0]: (T[1], {'article':'Artikel','quantity':'Menge'})}),
          list,
     )
 }

pprint(glom.glom(target2, spec2))


spec2 = {
     "items": (
          T.values(),
          Iter((T, {'article':'Artikel','quantity':'Menge'})),
          list,
     )
 }

pprint(glom.glom(target2, spec2))
              

data =     {'mgf':
    [
        {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Kopfsalat mit Herz', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 2.6},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Bio Patatli', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 3.5},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Zwiebeln', 'Menge': 0.452, 'Aktion': 0.0, 'Umsatz': 0.95},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Nidelchuechli', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 1.8},
     ],
    'lise':
    [
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Laugentessinerbrot', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 2.65},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'M-Drink Hoch Past', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 1.8},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Gesalzene Butter', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 2.25},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Bio Traubens. rot 1l', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 3.05},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Candida Mundwasser', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 3.9},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Candida Multicare 75ml', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 3.3},
     {'Datum_Zeit': Timestamp('2020-01-04 18:32:45'), 'Filiale': 'MM Altstetten', 'Kassennummer': 437, 'Transaktionsnummer': 6395, 'Artikel': 'Candida Anti Zahnstein', 'Menge': 1.0, 'Aktion': 0.0, 'Umsatz': 3.3},
     ]
            }

spec3 = {
     "items": (
          T.items(),
          Iter((S(value=T[0]),T[1],[{'buyer':S.value,'article':'Artikel','quantity':'Menge'}])).flatten(),
          list,
     )
 }



pprint(glom.glom(data,spec3))
