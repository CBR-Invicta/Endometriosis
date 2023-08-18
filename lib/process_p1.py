import json
from traceback import print_tb
from unittest import result
from unidecode import unidecode
from typing import Dict, List, Union, Any
import numpy as np
import pandas as pd
import re
from itertools import chain, product
from collections import Counter
from operator import itemgetter
import re
import shutup

from str_matching import match_words
from read_data import get_query

shutup.please()

# Lists

# BASIC_DATA
single_select_questions_BASIC_DATA = [
]
binary_questions_BASIC_DATA = []
distinct_questions_BASIC_DATA = [
    64,  # Czy posiada Pani dzieci biologiczne?
]
str_match_questions_BASIC_DATA = []
multiselect_questions_BASIC_DATA = []
numeric_questions_BASIC_DATA = []
genetic_questions_BASIC_DATA = []
map_questions_BASIC_DATA = []
pregnancy_questions_BASIC_DATA = []
binary_multiselect_BASIC_DATA = []
date_questions_BASIC_DATA = []
order_questions_BASIC_DATA = []
useless_questions_BASIC_DATA = [
    76,  # Zawód wykonywany
    356,  # Zawód wykonywany
    231,  # Z jakiej diagnostyki lub leczenia chciałaby Pani skorzystać?
    70,  # Z jakiej diagnostyki lub leczenia chciałaby Pani skorzystać?
    487,  # Jest Pani osobą aktywną sportowo?
    489,  # Czy jest Pani osobą muzykalną?
    274,  # Zdolności manualne
    62,  # Stan cywilny
    72,  # Wykształcenie
    354,  # Wykształcenie
    6,  # Imię w dniu urodzenia, jeżeli inne niż obecne
    8,  # Nazwisko w dniu urodzenia, jeżeli inne niż obecne
    12,  # Data urodzenia
    14,  # Miejsce urodzenia
    16,  # Obywatelstwo
    22,  # Kraj
    24,  # Miejscowość
    26,  # Kod pocztowy
    32,  # Kraj
    34,  # Miejscowość
    36,  # Kod pocztowy
    66,  # Czy jest Pani ubezpieczona w NFZ?
    68,  # Czy posiada Pani ubezpieczenie medyczne w inne...
    238,  # Preferowana forma kontaktu
    247,  # Kraj urodzenia
    248,  # Miejscowość urodzenia
    250,  # Kraj wydania dowodu tożsamości
    252,  # Województwo
    256,  # Preferowana forma kontaktu
    262,  # Znajomość języków obcych
    288,  # Jakie jest Pani wyznanie religijne?
    290,  # Adres korespondencyjny inny niż zamieszkania
    653,  # Obywatelstwo
    74,  # Zawód wyuczony
    258,  # Zawód wyuczony
    2, 4, 10, 651, 18, 20, 28, 30, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 358, 242
]
# DETAILED_INTERVIEW
single_select_questions_DETAILED_INTERVIEW = []
order_questions_DETAILED_INTERVIEW = []
binary_questions_DETAILED_INTERVIEW = [
    157,  # Czy czuje się Pani ogólnie zdrowa?
]
multiselect_questions_DETAILED_INTERVIEW = []
numeric_questions_DETAILED_INTERVIEW = [
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    360,
]
str_match_questions_DETAILED_INTERVIEW = [
    159,  # Czy aktualnie się pani leczy?
]
distinct_questions_DETAILED_INTERVIEW = []
genetic_questions_DETAILED_INTERVIEW = []
map_questions_DETAILED_INTERVIEW = []
pregnancy_questions_DETAILED_INTERVIEW = []
binary_multiselect_DETAILED_INTERVIEW = [
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
]
date_questions_DETAILED_INTERVIEW = []
useless_questions_DETAILED_INTERVIEW = []
# GENERAL_INTERVIEW_1
single_select_questions_GENERAL_INTERVIEW_1 = [
    # 213,  # Bóle okołomiesiączkowe
    # 214,  # Krwawienie miesięczne jest zwykle
]
order_questions_GENERAL_INTERVIEW_1 = [
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
]

binary_questions_GENERAL_INTERVIEW_1 = [
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # "Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    650,  # Czy obecnie karmi Pani piersią?
    220,  # Czy miesiączkuje Pani nieregularnie?
]
multiselect_questions_GENERAL_INTERVIEW_1 = [
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    # 655,  # Czy stosuje Pani środki antykoncepcyjne?
]
numeric_questions_GENERAL_INTERVIEW_1 = [
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
]
distinct_questions_GENERAL_INTERVIEW_1 = [
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?

]
pregnancy_questions_GENERAL_INTERVIEW_1 = [
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
]
binary_multiselect_GENERAL_INTERVIEW_1 = []
map_questions_GENERAL_INTERVIEW_1 = [
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
]
str_match_questions_GENERAL_INTERVIEW_1 = []
genetic_questions_GENERAL_INTERVIEW_1 = []
date_questions_GENERAL_INTERVIEW_1 = [
    116,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    216,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    459,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    117,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    217,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    461,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    435,  # Kiedy ostatni raz była Pani na wizycie u lekarza ginekologa?
    436,  # Kiedy miała Pani wykonywane USG ginekologiczne?
]
useless_questions_GENERAL_INTERVIEW_1 = [
    208,  # Czy miesiączkuje Pani regularnie?
    212,  # Data ostatniej miesiączki
]
# GENERAL_INTERVIEW_2
single_select_questions_GENERAL_INTERVIEW_2 = []
binary_questions_GENERAL_INTERVIEW_2 = [
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    484,  # Czy miała Pani wykonywany tatuaż, akupunkturę, zakładane kolczyki w ciągu ostatnich 3 miesięcy?

]
order_questions_GENERAL_INTERVIEW_2 = [
]
multiselect_questions_GENERAL_INTERVIEW_2 = [
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    143,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    497,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała

]
numeric_questions_GENERAL_INTERVIEW_2 = []
distinct_questions_GENERAL_INTERVIEW_2 = []
str_match_questions_GENERAL_INTERVIEW_2 = [
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy  jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
]
useless_questions_GENERAL_INTERVIEW_2 = [
    133,  # Czy kiedykolwiek była Pani w szpitalu z powodów medycznych innych niż zabiegi chirurgiczne?
    125,  # Czy jest Pani uczulona (na leki, pokarm, środki chemiczne, inne)?
    # Czy miała Pani alergie w wieku dziecięcym (których obecnie Pani nie ma)?
    127,
    135,  # Czy miała Pani kiedykolwiek złamane kości?
    # Ile dni w ciągu ostatnich 12 miesięcy nie mogła Pani pracować z powodu choroby (np. przeziębienia, grypy, zabiegów, wypadków)?
    137,
    141,  # Proszę dodać uwagi odnośnie zaznaczonych odpowiedzi
    493,  # Czy jest Pani uczulona (na leki, pokarm, środki chemiczne, inne)?
    495,  # Czy kiedykolwiek była Pani w szpitalu z powodów medycznych innych niż zabiegi chirurgiczne?
    648,  # Czy cierpi Pani na częste i powtarzające się migreny?
    # Czy ma Pani wadę wzroku (np. krótkowzroczność, dalekowzroczność, astygmatyzm)?
    649,


]
genetic_questions_GENERAL_INTERVIEW_2 = []
map_questions_GENERAL_INTERVIEW_2 = []
pregnancy_questions_GENERAL_INTERVIEW_2 = []
binary_multiselect_GENERAL_INTERVIEW_2 = []
date_questions_GENERAL_INTERVIEW_2 = []
# GENERAL_INTERVIEW_3
single_select_questions_GENERAL_INTERVIEW_3 = []
binary_questions_GENERAL_INTERVIEW_3 = [
    153,  # Czy kiedykolwiek stosowała Pani narkotyki?
    472,  # Czy kiedykolwiek stosowała Pani narkotyki?
    150,  # Czy pije Pani alkohol?
    644,  # Czy pije Pani alkohol?
]
order_questions_GENERAL_INTERVIEW_3 = [
]
multiselect_questions_GENERAL_INTERVIEW_3 = []
numeric_questions_GENERAL_INTERVIEW_3 = []
distinct_questions_GENERAL_INTERVIEW_3 = [
    151,  # Czy pali/paliła Pani papierosy?
    470,  # Czy pali/paliła Pani papierosy?

]
str_match_questions_GENERAL_INTERVIEW_3 = []
genetic_questions_GENERAL_INTERVIEW_3 = []
map_questions_GENERAL_INTERVIEW_3 = []
pregnancy_questions_GENERAL_INTERVIEW_3 = []
binary_multiselect_GENERAL_INTERVIEW_3 = []
date_questions_GENERAL_INTERVIEW_3 = []
useless_questions_GENERAL_INTERVIEW_3 = [
    152,  # Czy kiedykolwiek stosowała Pani dożylne, domięśniowe lub podskórne iniekcje leków z powodów niemedycznych?
    155,  # Czy kiedykolwiek narażona była Pani na nadmierne ilości szkodliwych czynników fizycznych, chemicznych lub biologicznych?
    471,  # Czy kiedykolwiek stosowała Pani dożylne, domięśniowe lub podskórne iniekcje leków z powodów niemedycznych?
    499,  # Czy kiedykolwiek była Pani karana?
    501,  # Czy przebywała Pani w areszcie lub więzieniu dłużej niż 72 godziny w ciągu ostatnich 6 miesięcy?
    505,  # Czy kiedykolwiek narażona była Pani na nadmierne ilości szkodliwych czynników fizycznych, chemicznych lub biologicznych?
    154,  # Proszę zaznaczyć wszystkie środki, które Pani kiedykolwiek stosowała
    503,  # Proszę zaznaczyć wszystkie środki, które Pani kiedykolwiek stosowała
]
# Genetics :
single_select_questions_GENETICS = []
order_questions_GENETICS = [
]
binary_questions_GENETICS = [
    196,  # Czy była Pani adoptowana?
    # Czy urodziła się Pani z jakąkolwiek wadą wrodzoną (wady serca, rozszczep wargi lub podniebienia itp.)?
    197,
    198,  # Czy występują w Pani rodzinie jakieś znane Pani choroby genetyczne lub wady wrodzone?
    199,  # Czy urodziło się w Pani rodzinie dziecko z chorobą genetyczną lub wadą wrodzoną?
    507,  # Czy w Pani rodzinie występowały niepowodzenia rozrodu?
    # Czy w Pani rodzinie występowały poronienia samoistne (>2 u jednej kobiety)?
    509,
    511,  # Czy w Pani rodzinie występowały urodzenia martwego płodu?
    513,  # Czy w Pani rodzinie występowała niepłodność?
    515,  # Czy w Pani rodzinie występowały poważne wady genetyczne?
    517,  # Czy w Pani rodzinie występowały wady wrodzone wykrywane w trakcie ciąży u płodu?
    521,  # Czy w Pani rodzinie występowały choroby genetycznie uwarunkowane lub choroby, które miały niewyjaśnione podłoże?
    523,  # Czy w Pani rodzinie są różne osoby, które chorowały na choroby nowotworowe w podobny sposób?
    525,  # Czy w Pani rodzinie są osoby z niepełnosprawnością umysłową/upośledzeniem umysłowym o niewyjaśnionej przyczynie?
    527,  # Czy u kogoś w Pani rodzinie wystąpiły nieprawidłowości rozwoju płciowego?
    # Czy w Pani rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?
    529,
    531,  # Czy w Pani rodzinie wystąpiły przypadki translokacyjnego zespołu Downa?
    533,  # Czy w Pani rodzinie wykryto przypadki aberracji chromosomowej lub nietypowego wariantu chromosomowego w badaniu prenatalnym u płodu?
    535,  # Czy w Pani rodzinie wystąpiły przypadki nietypowego wariantu chromosomowego?
    537,  # Czy w Pani rodzinie wystąpiły przypadki pierwotnego lub wtórnego braku miesiączki?
    539,  # Czy w Pani rodzinie wystąpiły przypadki azoospermii lub oligozoospermia w badaniu nasienia?
    541,  # Czy w Pani rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?
    # Czy u Pani lub w Pani rodzinie wystąpiły przypadki zespołu niestabilności chromosomów  (Zespół Blooma, Anemia Fanconiego, Ataxia Teleangiectasia, zespół Nijmegen)?
    547,
    549,  # Czy w Pani rodzinie wystąpiły przypadki mutacji punktowych, delecji, duplikacji w obrębie genu/ów?
    551,  # Czy w Pani rodzinie wystąpiły przypadki wykrycia mutacji punktowych, delecji, duplikacji w obrębie genu/genów w badaniu prenatalnym u płodu pacjentki?
    553,  # Czy w Pani rodzinie wystąpiły przypadki chorób mitochondrialnych?
    555,  # Czy w Pani rodzinie wykryto u płodu w badaniu prenatalnym choroby mitochondrialne?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    # Czy stwierdzono u Pani: wrodzony hipogonadyzm, zaburzenia rozwoju narządów płciowych, nieprawidłowy rozwój trzeciorzędowych cech płciowych (takich jak niedorozwój piersi, nieprawidłowa budowa ciała i nieprawidłowe – nadmierne owłosienie)?
    567,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    570,  # Jeśli wystąpiły poronienia to, czy zbadano kariotyp martwego płodu z poronienia?
    571,  # Czy wystąpiły u Pani urodzenia martwego płodu?
    573,  # Czy była/jest Pani narażona na działanie promieniowania jonizującego, promieni rentgena, radioterapia?
    474,  # Czy była Pani adoptowana?

]
multiselect_questions_GENETICS = []
numeric_questions_GENETICS = []
distinct_questions_GENETICS = []
str_match_questions_GENETICS = []
map_questions_GENETICS = []
pregnancy_questions_GENETICS = []
binary_multiselect_GENETICS = []
date_questions_GENETICS = []
genetic_questions_GENETICS = [
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane  z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,

]
useless_questions_GENETICS = [
    598,  # Czy w Pani rodzinie wystąpiły przypadki znacznego zaburzenia wzrostu, niski wzrost u kobiety?
    572,  # Czy była/jest Pani narażona na działanie związków mutagennych?
    563,  # Czy jest Pani spokrewniona ze współmałżonkiem/partnerem?
    565,  # Czy występują u Pani wrodzone zmiany morfologiczne budowy ciała?
    # Czy w Pani rodzinie wystąpiły zaburzenia wzrostu (np. niski wzrost) lub nieproporcjonalny wzrost?
    543,
    545,  # Czy w Pani rodzinie wystąpiły przypadki znacznego zaburzenia wzrostu, wysoki wzrost u mężczyzny?
    519,  # Czy w Pani rodzinie występowały małżeństwa między krewnymi?
    200,  # Czy jest Pani rasy kaukaskiej?
    202,  # Czy jest Pani rasy negroidalnej?
    203,  # Czy jest Pani pochodzenia śródziemnomorskiego
    201,  # Czy jest Pani pochodzenia żydowskiego?
    232,  # Czy jest Pani pochodzenia żydowskiego?
    374,  # Matka
    376,  # Ojciec
    378,  # Liczba braci
    380,  # Liczba sióstr
    382,  # Liczba synów
    384,  # Liczba córek
    386,  # Matka matki
    388,  # Ojciec matki
    390,  # Liczba braci matki
    392,  # Liczba sióstr matki
    394,  # Matka ojca
    396,  # Ojciec ojca
    398,  # Liczba braci ojca
    400,  # Liczba sióstr ojca
]

# PRE_INTERVIEW
single_select_questions_PRE_INTERVIEW = [
    # 102,  # Jak długo stara się Pani zajść w ciążę?
    # 104,  # Jak często odbywa Pani stosunki płciowe?
]
order_questions_PRE_INTERVIEW = [
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
]
binary_questions_PRE_INTERVIEW = []
multiselect_questions_PRE_INTERVIEW = []
numeric_questions_PRE_INTERVIEW = []

distinct_questions_PRE_INTERVIEW = [
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
]
str_match_questions_PRE_INTERVIEW = []
map_questions_PRE_INTERVIEW = []
pregnancy_questions_PRE_INTERVIEW = []
binary_multiselect_PRE_INTERVIEW = []
date_questions_PRE_INTERVIEW = []
genetic_questions_PRE_INTERVIEW = []

useless_questions_PRE_INTERVIEW = [
    215,  # Czy w trakcie dotychczasowej diagnostyki miała Pani wykonywane jakiekolwiek badania?
    106,  # Czy w trakcie dotychczasowej diagnostyki miała Pani wykonywane jakiekolwiek badania?
    236,  # Czy w trakcie dotychczasowej diagnostyki miała Pani wykonywane jakiekolwiek badania?
    366,  # Czy oddała Pani komórki w programach dawstwa?
    229,  # Przyczyna zgłoszenia się do kliniki
    368,  # Powód, dla którego chciałaby Pani wziąć udział w programie dawstwa
    98,  # Przyczyna zgłoszenia się do kliniki
    363,  # Czy kiedykolwiek starała się Pani, by zostać dawczynią komórek jajowych?
    364,  # Czy została Pani kiedykolwiek zakwalifikowana jako dawczyni komórek jajowych?
    365,  # Czy jest Pani na jakiejkolwiek liście dostępnych dawczyń komórek jajowych?
    369,  # Czy wzięłaby Pani udział w programie dawstwa, gdyby nie było zwrotu pieniędzy za poniesione koszty i poświęcony czas?
    # Czy wyraziłaby Pani zgodę na podróż do kliniki znajdującej się poza Pani miejscem zamieszkania (koszty podróży byłyby zwracane) w celu pobrania komórek jajowych?
    370,
    371,  # Czy jeżeli dziecko poczęte z Pani komórek jajowych urodzi się z poważnymi wadami rozwojowymi lub wadami genetycznymi i jeżeli medycznie potwierdzimy, że Pani przyszłe lub obecne dzieci mogą mieć zwiększone ryzyko wystąpienia nieprawidłowości genetycznych i wskazane byłoby przeprowadzenie odpowiednich badań dzieciom lub badania prenatalnego w czasie Pani ciąży, chciałaby Pani zostać o tym poinformowana?
    # Czy jeżeli w przyszłości u kogoś z Pani rodziny zostaną rozpoznane zaburzenia (choroby) o podłożu genetycznym, poinformuje nas Pani o takiej sytuacji?
    372,
    100,  # Czy leczyła się Pani z powodu określonego wyżej?
    362,  # Przyczyna zgłoszenia się do Kliniki
]

# PHENOTYPIC_DATA

single_select_questions_PHENOTYPIC_DATA = [
    617,  # Grupa krwi
    619,  # Czynnik Rh
]
order_questions_PHENOTYPIC_DATA = []
binary_questions_PHENOTYPIC_DATA = []
multiselect_questions_PHENOTYPIC_DATA = []
numeric_questions_PHENOTYPIC_DATA = [
    90,  # Waga (treshold 46-120)
]

distinct_questions_PHENOTYPIC_DATA = [
    92,  # Wzrost
    86,  # Grupa krwi
    88,  # Czynnik Rh
]

str_match_questions_PHENOTYPIC_DATA = []
map_questions_PHENOTYPIC_DATA = []
pregnancy_questions_PHENOTYPIC_DATA = []
binary_multiselect_PHENOTYPIC_DATA = []
date_questions_PHENOTYPIC_DATA = []
genetic_questions_PHENOTYPIC_DATA = []

useless_questions_PHENOTYPIC_DATA = [
    84,  # Struktura włosów
    82,  # Naturalny kolor włosów
    80,  # Kolor oczu
    609,  # Etniczność
    78,  # Rasa
    94,  # Budowa ciała
    292,  # Rasa
    296,  # Budowa ciała
    607,  # Kolor skóry
    611,  # Kolor oczu
    613,  # Nauralny kolor włosów
    615,  # Struktura włosów
    # Etniczność (input - wszystkie odpowiedzi grupa etniczna: wschodnio-europejska)
    96,
]


single_select_questions_STATEMENTS = []
order_questions_STATEMENTS = []
binary_questions_STATEMENTS = []
multiselect_questions_STATEMENTS = []
numeric_questions_STATEMENTS = []
distinct_questions_STATEMENTS = []
str_match_questions_STATEMENTS = []
map_questions_STATEMENTS = []
pregnancy_questions_STATEMENTS = []
binary_multiselect_STATEMENTS = []
date_questions_STATEMENTS = []
genetic_questions_STATEMENTS = []
useless_questions_STATEMENTS = [206, 640]

single_select_questions_FREESTYLE = []
order_questions_FREESTYLE = []
binary_questions_FREESTYLE = []
multiselect_questions_FREESTYLE = []
numeric_questions_FREESTYLE = []
distinct_questions_FREESTYLE = []
str_match_questions_FREESTYLE = []
map_questions_FREESTYLE = []
pregnancy_questions_FREESTYLE = []
binary_multiselect_FREESTYLE = []
date_questions_FREESTYLE = []
genetic_questions_FREESTYLE = []
useless_questions_FREESTYLE = [642, 638, 636, 634]

mapping_answers = [
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    129, 462, 240, 159,  # str matching questions
    581, 131,
    404, 406, 408, 575, 577, 579, 587, 589, 593, 601, 603, 605,
    139, 108
]
# subset lists for different models

leki = [
    64,  # Czy posiada Pani dzieci biologiczne?
    # 356,  # Zawód wykonywany
    # 76,  # Zawód wykonywany
    157,  # Czy czuje się Pani ogólnie zdrowa?
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    # 360,
    159,  # Czy aktualnie się pani leczy?
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    # 650,  # Czy obecnie karmi Pani piersią?
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    655,  # Czy stosuje Pani środki antykoncepcyjne?
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  #  Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
    220,  # Czy miesiączkuje Pani nieregularnie?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy     jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    143,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    497,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    # Czy w Pani rodzinie występowały poronienia samoistne (>2 u jednej kobiety)?
    509,
    511,  # Czy w Pani rodzinie występowały urodzenia martwego płodu?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane      z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    90,  # Waga (treshold 46-120)
    92,  # Wzrost
    86,  # Grupa krwi
    617,  # Grupa krwi
    88,  # Czynnik Rh
    619,  # Czynnik Rh
]

zalecenia_zlecenia = [
    64,  # Czy posiada Pani dzieci biologiczne?
    # 356,  # Zawód wykonywany
    # 76,  # Zawód wykonywany
    157,  # Czy czuje się Pani ogólnie zdrowa?
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    # 360,
    159,  # Czy aktualnie się pani leczy?
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    # 650,  # Czy obecnie karmi Pani piersią?
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    655,  # Czy stosuje Pani środki antykoncepcyjne?
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
    116,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    117,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    216,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    217,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    220,  # Czy miesiączkuje Pani nieregularnie?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?
    435,  # Kiedy ostatni raz była Pani na wizycie u lekarza ginekologa?
    436,  # Kiedy miała Pani wykonywane USG ginekologiczne?
    459,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    461,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy     jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    143,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    484,  # Czy miała Pani wykonywany tatuaż, akupunkturę, zakładane kolczyki w ciągu ostatnich 3 miesięcy?
    497,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    153,  # Czy kiedykolwiek stosowała Pani narkotyki?
    472,  # Czy kiedykolwiek stosowała Pani narkotyki?
    150,  # Czy pije Pani alkohol?
    644,  # Czy pije Pani alkohol?
    151,  # Czy pali/paliła Pani papierosy?
    470,  # Czy pali/paliła Pani papierosy?
    196,  # Czy była Pani adoptowana?
    # Czy urodziła się Pani z jakąkolwiek wadą wrodzoną (wady serca, rozszczep wargi lub podniebienia itp.)?
    197,
    198,  # Czy występują w Pani rodzinie jakieś znane Pani choroby genetyczne lub wady wrodzone?
    199,  # Czy urodziło się w Pani rodzinie dziecko z chorobą genetyczną lub wadą wrodzoną?
    507,  # Czy w Pani rodzinie występowały niepowodzenia rozrodu?
    # Czy w Pani rodzinie występowały poronienia samoistne (>2 u jednej kobiety)?
    509,
    511,  # Czy w Pani rodzinie występowały urodzenia martwego płodu?
    513,  # Czy w Pani rodzinie występowała niepłodność?
    515,  # Czy w Pani rodzinie występowały poważne wady genetyczne?
    517,  # Czy w Pani rodzinie występowały wady wrodzone wykrywane w trakcie ciąży u płodu?
    521,  # Czy w Pani rodzinie występowały choroby genetycznie uwarunkowane lub choroby, które miały niewyjaśnione podłoże?
    523,  # Czy w Pani rodzinie są różne osoby, które chorowały na choroby nowotworowe w podobny sposób?
    525,  # Czy w Pani rodzinie są osoby z niepełnosprawnością umysłową/upośledzeniem umysłowym o niewyjaśnionej przyczynie?
    527,  # Czy u kogoś w Pani rodzinie wystąpiły nieprawidłowości rozwoju płciowego?
    # Czy w Pani rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?
    529,
    531,  # Czy w Pani rodzinie wystąpiły przypadki translokacyjnego zespołu Downa?
    533,  # Czy w Pani rodzinie wykryto przypadki aberracji chromosomowej lub nietypowego wariantu chromosomowego w badaniu prenatalnym u płodu?
    535,  # Czy w Pani rodzinie wystąpiły przypadki nietypowego wariantu chromosomowego?
    537,  # Czy w Pani rodzinie wystąpiły przypadki pierwotnego lub wtórnego braku miesiączki?
    539,  # Czy w Pani rodzinie wystąpiły przypadki azoospermii lub oligozoospermia w badaniu nasienia?
    541,  # Czy w Pani rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?
    # Czy u Pani lub w Pani rodzinie wystąpiły przypadki zespołu niestabilności chromosomów     (Zespół Blooma, Anemia Fanconiego, Ataxia Teleangiectasia, zespół Nijmegen)?
    547,
    549,  # Czy w Pani rodzinie wystąpiły przypadki mutacji punktowych, delecji, duplikacji w obrębie genu/ów?
    551,  # Czy w Pani rodzinie wystąpiły przypadki wykrycia mutacji punktowych, delecji, duplikacji w obrębie genu/genów w badaniu prenatalnym u płodu pacjentki?
    553,  # Czy w Pani rodzinie wystąpiły przypadki chorób mitochondrialnych?
    555,  # Czy w Pani rodzinie wykryto u płodu w badaniu prenatalnym choroby mitochondrialne?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    # Czy stwierdzono u Pani: wrodzony hipogonadyzm, zaburzenia rozwoju narządów płciowych, nieprawidłowy rozwój trzeciorzędowych cech płciowych (takich jak niedorozwój piersi, nieprawidłowa budowa ciała i nieprawidłowe – nadmierne owłosienie)?
    567,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    570,  # Jeśli wystąpiły poronienia to, czy zbadano kariotyp martwego płodu z poronienia?
    571,  # Czy wystąpiły u Pani urodzenia martwego płodu?
    572,  # Czy była/jest Pani narażona na działanie związków mutagennych?
    573,  # Czy była/jest Pani narażona na działanie promieniowania jonizującego, promieni rentgena, radioterapia?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane      z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,
    474,  # Czy była Pani adoptowana?
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    617,  # Grupa krwi
    619,  # Czynnik Rh
    90,  # Waga (treshold 46-120)
    92,  # Wzrost
    86,  # Grupa krwi
    617,  # Grupa krwi
    88,  # Czynnik Rh
]

IDC10 = [
    64,  # Czy posiada Pani dzieci biologiczne?
    # 356,  # Zawód wykonywany
    # 76,  # Zawód wykonywany
    157,  # Czy czuje się Pani ogólnie zdrowa?
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    # 360,
    159,  # Czy aktualnie się pani leczy?
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    # 650,  # Czy obecnie karmi Pani piersią?
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    655,  # Czy stosuje Pani środki antykoncepcyjne?
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
    220,  # Czy miesiączkuje Pani nieregularnie?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy     jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    572,  # Czy była/jest Pani narażona na działanie związków mutagennych?
    573,  # Czy była/jest Pani narażona na działanie promieniowania jonizującego, promieni rentgena, radioterapia?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane      z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    90,  # Waga (treshold 46-120)
    92,  # Wzrost
    86,  # Grupa krwi
    617,  # Grupa krwi
    88,  # Czynnik Rh
    619,  # Czynnik Rh

]
diagnoza_z_ankiety_kwalifikacyjnej = [
    64,  # Czy posiada Pani dzieci biologiczne?
    # 356,  # Zawód wykonywany
    # 76,  # Zawód wykonywany
    157,  # Czy czuje się Pani ogólnie zdrowa?
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    # 360,
    159,  # Czy aktualnie się pani leczy?
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    # 650,  # Czy obecnie karmi Pani piersią?
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    655,  # Czy stosuje Pani środki antykoncepcyjne?
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
    220,  # Czy miesiączkuje Pani nieregularnie?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy     jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    572,  # Czy była/jest Pani narażona na działanie związków mutagennych?
    573,  # Czy była/jest Pani narażona na działanie promieniowania jonizującego, promieni rentgena, radioterapia?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane      z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    90,  # Waga (treshold 46-120)
    92,  # Wzrost
    86,  # Grupa krwi
    617,  # Grupa krwi
    88,  # Czynnik Rh
    619,  # Czynnik Rh

]

nastepny_krok_procesu = [
    64,  # Czy posiada Pani dzieci biologiczne?
    356,  # Zawód wykonywany
    76,  # Zawód wykonywany
    157,  # Czy czuje się Pani ogólnie zdrowa?
    161,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z ogólnym stanem zdrowia?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    163,
    165,  # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    167,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    169,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    171,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    173,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    175,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    177,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    179,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdia itp.)?
    181,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    183,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    185,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    187,
    # Czy jest Pani zestresowana? (prosimy o wskazanie poziomu stresu w skali od 1 do 10, gdzie 1 - oznacza minimalny, a 10 - maksymalny poziom stresu)
    # 360,
    159,  # Czy aktualnie się pani leczy?
    213,  # Bóle okołomiesiączkowe
    214,  # Krwawienie miesięczne jest zwykle
    114,  # Czy kiedykolwiek rozpoznano u Pani niepłodność?
    433,  # Czy ma Pani dolegliwości okołomiesiączkowe?
    434,  # Czy występują u Pani krwawienia lub plamienia
    437,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    445,  # Czy kiedykolwiek rozpoznano u Pani zaburzenia płodności?
    # 650,  # Czy obecnie karmi Pani piersią?
    594,  # Czy stosuje Pani środki antykoncepcyjne?
    655,  # Czy stosuje Pani środki antykoncepcyjne?
    112,  # W jakim wieku miała Pani pierwszą miesiączkę?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    209,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    210,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    211,
    479,  # obecnie
    480,  # w ciągu ostatnich 6 miesięcy
    482,  # w sumie przez całe dotychczasowe życie
    439,  # Czy stosuje Pani lub stosowała środki antykoncepcyjne?
    119,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    223,  # Jakie stosuje Pani lub stosowała środki antykoncepcyjne?
    121,  # Ile razy była Pani w ciąży?
    222,  # Ile razy była Pani w ciąży?
    442,  # Ile razy była Pani w ciąży?
    # Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    429,
    # Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    430,
    # Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)
    431,
    432,  # Średnia liczba dni krwawienia miesięcznego
    116,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    117,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    216,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    217,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    220,  # Czy miesiączkuje Pani nieregularnie?
    427,  # W jakim wieku miała Pani pierwszą miesiączkę?
    428,  # Czy miesiączkuje Pani nieregularnie?
    435,  # Kiedy ostatni raz była Pani na wizycie u lekarza ginekologa?
    436,  # Kiedy miała Pani wykonywane USG ginekologiczne?
    459,  # Czy kiedykolwiek miała Pani wykonywaną cytologię?
    461,  # Czy kiedykolwiek miała Pani wykonywane USG piersi lub mammografię?
    447,  # Czy kiedykolwiek przyjmowała Pani leki przeciwdepresyjne dłużej niż 3 miesiące
    483,  # Czy przeszła Pani zabieg chirurgicznego wycięcia jednego lub obu jajników?
    485,  # Czy miała Pani transfuzję krwi lub składników krwiopochodnych w ciągu ostatnich 10 lat?
    139,  # Jeśli obecnie choruje Pani, kiedykolwiek chorowała lub była Pani kiedykolwiek leczona z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    # Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    123,
    491,  # Czy przyjmuje Pani obecnie lub przyjmowała Pani w ciągu ostatnich 12 miesięcy     jakiekolwiek leki?
    129,  # Czy jest Pani lub była pod opieką lekarza z powodu leczenia jakichś chorób?
    # Czy miała Pani w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie torbieli jajnikowych, itp.)?
    131,
    240,  # Czy jest Pani pod stałą opieką lekarza?
    462,  # Czy jest Pani lub była Pani pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    143,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    484,  # Czy miała Pani wykonywany tatuaż, akupunkturę, zakładane kolczyki w ciągu ostatnich 3 miesięcy?
    497,  # Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała
    196,  # Czy była Pani adoptowana?
    # Czy urodziła się Pani z jakąkolwiek wadą wrodzoną (wady serca, rozszczep wargi lub podniebienia itp.)?
    197,
    198,  # Czy występują w Pani rodzinie jakieś znane Pani choroby genetyczne lub wady wrodzone?
    199,  # Czy urodziło się w Pani rodzinie dziecko z chorobą genetyczną lub wadą wrodzoną?
    507,  # Czy w Pani rodzinie występowały niepowodzenia rozrodu?
    # Czy w Pani rodzinie występowały poronienia samoistne (>2 u jednej kobiety)?
    509,
    511,  # Czy w Pani rodzinie występowały urodzenia martwego płodu?
    513,  # Czy w Pani rodzinie występowała niepłodność?
    515,  # Czy w Pani rodzinie występowały poważne wady genetyczne?
    517,  # Czy w Pani rodzinie występowały wady wrodzone wykrywane w trakcie ciąży u płodu?
    521,  # Czy w Pani rodzinie występowały choroby genetycznie uwarunkowane lub choroby, które miały niewyjaśnione podłoże?
    523,  # Czy w Pani rodzinie są różne osoby, które chorowały na choroby nowotworowe w podobny sposób?
    525,  # Czy w Pani rodzinie są osoby z niepełnosprawnością umysłową/upośledzeniem umysłowym o niewyjaśnionej przyczynie?
    527,  # Czy u kogoś w Pani rodzinie wystąpiły nieprawidłowości rozwoju płciowego?
    # Czy w Pani rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?
    529,
    531,  # Czy w Pani rodzinie wystąpiły przypadki translokacyjnego zespołu Downa?
    533,  # Czy w Pani rodzinie wykryto przypadki aberracji chromosomowej lub nietypowego wariantu chromosomowego w badaniu prenatalnym u płodu?
    535,  # Czy w Pani rodzinie wystąpiły przypadki nietypowego wariantu chromosomowego?
    537,  # Czy w Pani rodzinie wystąpiły przypadki pierwotnego lub wtórnego braku miesiączki?
    539,  # Czy w Pani rodzinie wystąpiły przypadki azoospermii lub oligozoospermia w badaniu nasienia?
    541,  # Czy w Pani rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?
    # Czy u Pani lub w Pani rodzinie wystąpiły przypadki zespołu niestabilności chromosomów     (Zespół Blooma, Anemia Fanconiego, Ataxia Teleangiectasia, zespół Nijmegen)?
    547,
    549,  # Czy w Pani rodzinie wystąpiły przypadki mutacji punktowych, delecji, duplikacji w obrębie genu/ów?
    551,  # Czy w Pani rodzinie wystąpiły przypadki wykrycia mutacji punktowych, delecji, duplikacji w obrębie genu/genów w badaniu prenatalnym u płodu pacjentki?
    553,  # Czy w Pani rodzinie wystąpiły przypadki chorób mitochondrialnych?
    555,  # Czy w Pani rodzinie wykryto u płodu w badaniu prenatalnym choroby mitochondrialne?
    557,  # Czy choruje Pani na chorobę nowotworową?
    559,  # Czy chorowała Pani na chorobę nowotworową?
    # Czy chorowała Pani na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    561,
    # Czy stwierdzono u Pani: wrodzony hipogonadyzm, zaburzenia rozwoju narządów płciowych, nieprawidłowy rozwój trzeciorzędowych cech płciowych (takich jak niedorozwój piersi, nieprawidłowa budowa ciała i nieprawidłowe – nadmierne owłosienie)?
    567,
    568,  # Czy stwierdzono u Pani przedwczesne wygaśnięcie funkcji jajnika?
    # Czy stwierdzono u Pani: pierwotny brak miesiączki, nawracające straty ciąż, nawracające poronienia (2 lub więcej poronień)?
    569,
    570,  # Jeśli wystąpiły poronienia to, czy zbadano kariotyp martwego płodu z poronienia?
    571,  # Czy wystąpiły u Pani urodzenia martwego płodu?
    572,  # Czy była/jest Pani narażona na działanie związków mutagennych?
    573,  # Czy była/jest Pani narażona na działanie promieniowania jonizującego, promieni rentgena, radioterapia?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)
    404,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)
    406,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)
    575,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)
    577,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)
    579,
    # Czy kiedykolwiek zaobserwowała Pani niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    581,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    587,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)
    589,
    593,  # Czy kiedykolwiek zaobserwowała Pani inne niepokojące objawy?
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane      z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    601,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    603,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    605,
    # Czy kiedykolwiek zaobserwowała Pani u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    408,
    474,  # Czy była Pani adoptowana?
    102,  # Jak długo stara się Pani zajść w ciążę?
    104,  # Jak często odbywa Pani stosunki płciowe?
    110,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miała Pani wykonywane jakiekolwiek procedury medyczne (np. udrożnienie jajowodów, zapłodnienie pozaustrojowe)?
    108,
    226,  # Czy przyjmowała Pani lub przyjmuje jakiekolwiek leki?
    617,  # Grupa krwi
    619,  # Czynnik Rh
    90,  # Waga (treshold 46-120)
    92,  # Wzrost
    86,  # Grupa krwi
    617,  # Grupa krwi
    88,  # Czynnik Rh
]

# configs

drop_genetic_cols = [
    "406_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_oddechowym_(np._zapalenia_oskrzeli,_niewydolność_płuc_itp.)?:PNEUMONIA",
    "577_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_endokrynnym/metabolicznym_(np._powiększenie_tarczycy,_nietolerancja_ciepła_itp.)?:GOITRE",
    "577_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_endokrynnym/metabolicznym_(np._powiększenie_tarczycy,_nietolerancja_ciepła_itp.)?:SHORT_STATURE",
    "581_Czy_kiedykolwiek_zaobserwowała_Pani_niepokojące_objawy_związane_z_układem_płciowym_(np._brak_miesiączki,_trudności_w_zapłodnieniu_itp.)?:CRYPTORCHIDISM",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:ASTIGMATISM",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:FARSIGHTEDNESS",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:GLASSES_OR_CONTACT_LENSES_BEFORE_45",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:DEVIATED_NASAL_SEPTUM",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:NO_SMELL",
    "587_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:DALTONISM",
    "589_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_ze_skórą_lub_włosami_(np._bardzo_sucha_skóra,_nadmiernie_wypadające_włosy_itp.)?:MULTIPLE_MOLES",
    "589_Czy_kiedykolwiek_zaobserwowała_Pani_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_ze_skórą_lub_włosami_(np._bardzo_sucha_skóra,_nadmiernie_wypadające_włosy_itp.)?:LARGE_DEEP_VEIN_VARICOSE",
    "593_Czy_kiedykolwiek_zaobserwowała_Pani_inne_niepokojące_objawy?:ABNORMAL_BITE",
    "593_Czy_kiedykolwiek_zaobserwowała_Pani_inne_niepokojące_objawy?:EARLY_DEATH",
    "593_Czy_kiedykolwiek_zaobserwowała_Pani_inne_niepokojące_objawy?:OTHER_SITUATION",
]

str_match_questions_settings = {
    129: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "ENDOMETRIOZA",
            "DEPRESJA",
            "ASTMA",
            "PCOS",
            "POLICYSTYCZNYCH",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
        ],
    },
    462: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "ENDOMETRIOZA",
            "DEPRESJA",
            "ASTMA",
            "PCOS",
            "POLICYSTYCZNYCH",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
        ],
    },
    240: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "ENDOMETRIOZA",
            "DEPRESJA",
            "ASTMA",
            "PCOS",
            "POLICYSTYCZNYCH",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
        ],
    },
    123: {
        "pattern": '{"user": \[\["([^"]*)"',
        "list_of_classes": [
            "ACARD",
            # "ASERTIN",
            # "ASENTRA",
            "BROMERGON",
            # "BELARA",
            "CLOSTILBEGYT",
            # "CIPRONEX",
            "D3",
            "D",
            "DUPHASTON",
            # "DOPEGYT",
            "DOSTINEX",
            "EUTHYROX",
            # "ENCORTON",
            # "ESTROFEM",
            "FERTISTIM",
            "FOLIOWY",
            "FOLIK",
            "FEMIBION",
            # "FURAGINA",
            "GLUCOPHAGE",
            # "LAMETTA",
            "LETROX",
            "LUTEINA",
            "INOFEM",
            # "MAGNEZ",
            "METFORMAX",
            "MENOPUR",
            "MIOVELIA",
            "NEOPARIN",
            "OVARIN",
            # "OVITRELLE",
            "PREGNA",
            "PRENATAL",
            # "PREGABALINA",
            "SIOFOR",
            # "TRITTICO",
            # "TARDYFERON",
            # "VIBIN",
            # "ZOLOFT",
        ],
    },
    491: {
        "pattern": '{"user": \[\["([^"]*)"',
        "list_of_classes": [
            "ACARD",
            # "ASERTIN",
            # "ASENTRA",
            "BROMERGON",
            # "BELARA",
            "CLOSTILBEGYT",
            # "CIPRONEX",
            "D3",
            "D",
            "DUPHASTON",
            # "DOPEGYT",
            "DOSTINEX",
            "EUTHYROX",
            # "ENCORTON",
            # "ESTROFEM",
            "FERTISTIM",
            "FOLIOWY",
            "FOLIK",
            "FEMIBION",
            # "FURAGINA",
            "GLUCOPHAGE",
            # "LAMETTA",
            "LETROX",
            "LUTEINA",
            "INOFEM",
            # "MAGNEZ",
            "METFORMAX",
            "MENOPUR",
            "MIOVELIA",
            "NEOPARIN",
            "OVARIN",
            # "OVITRELLE",
            "PREGNA",
            "PRENATAL",
            # "PREGABALINA",
            "SIOFOR",
            # "TRITTICO",
            # "TARDYFERON",
            # "VIBIN",
            # "ZOLOFT",
        ],
    },
    159: {
        "pattern": None,
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "ENDOMETRIOZA",
            "DEPRESJA",
            "ASTMA",
            "PCOS",
            "POLICYSTYCZNYCH",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
        ],
    },
    131: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "LAPAROSKOPIA",
            "CESARSKIE",
            "HISTEROSKOPIA",
            "WYROSTEK",
            "LAPAROTOMIA",
            "MIGDALKOW",
            "ZOLCIOWEGO",
            "TORBIELI",
            "KONIZACJA",
            "PRZEPUKLINA",
            "LYZECZKOWANIE",
            "POLIPA",
            "MIESNIAKA",
            "POZAMACICZNEJ",
            "JAJOWODU",
            "CESARKA",
            "HISTEROLAPAROSKOPIA",
            "APPENDEKTOMIA",
            "HSG",
            "ENDOMETRIOZA",
            # "CHOLECYSTEKTOMIA",
            "ABLACJA",
        ],
    },
}

threshold = {
    64: {"max": 7},
    92: {"min": 138, "max": 194},
    90: {"min": 46, "max": 120},
    112: {"min": 9, "max": 18},
    209: {"min": 18, "max": 100},
    210: {"min": 18, "max": 100},
    211: {"min": 18, "max": 100},
    479: {"min": 0, "max": 1},
    480: {"min": 0, "max": 5},
    482: {"min": 0, "max": 25},
}

mapping_order_scale = {
    "SCARCE_BLEEDING": 0,
    "AVERAGE_BLEEDING": 1,
    "PLENTIFUL_BLEEDING": 2,
    "DO_NOT_OCCUR": 0,
    "MODERATELY_SEVERE": 1,
    "VERY_STRONG_CRAMPS": 2,
    "SINCE_FEW_MONTHS": 0,
    "SINCE_FEW_YEARS": 1,
    "MULTIPLE_YEARS": 2,
    "SEVERAL_TIMES_DAY": 3,
    "ONE_OR_SEVERAL_TIMES_DAY": 3,
    "FEW_TIMES_WEEK": 2,
    "OCCASIONALLY": 0,
    "SEVERAL_TIMES_MONTH": 1,
}
# Functions


def match_to_columns(final_df: pd.DataFrame, response: pd.Series) -> pd.DataFrame:
    """match_to_columns dodaje wartości, jeżeli nazwa kolumny występuje w pd.Series zawierającej wpisy pacjentów


    Parameters
    ----------
    final_df : pd.DataFrame
        Zbiór danych, którego kolumny odpowiadają ostatecznym wartościom wyszukiwanym przez string matching.
    response : pd.Series
        Wpisy pacjentów. Wartości w pd.Series powinny być typu List

    Returns
    -------
    pd.DataFrame
        Wypełniony final_df
    """
    for column_name in final_df:
        for i in response.index:
            list_of_words = response.loc[i]
            if len(list_of_words) > 0:
                matches = []
                for word in response.loc[i]:
                    if word != "":
                        single_words = word.split(" ")
                        for sub_word in single_words:
                            if sub_word != "":
                                matches.append(
                                    match_words(
                                        column_name, sub_word, result_threshold=0.8
                                    )[0]
                                    * 1
                                )
                final_df.loc[i][column_name] = (sum(matches) > 0) * 1
    return final_df


# process_str_match_questions
def process_str_match_questions(
    data: pd.DataFrame,
    question_id: int,
    str_match_questions_settings: Dict[
        float, Dict[str, Any]
    ] = str_match_questions_settings,
    fillna:bool = True,
) -> pd.DataFrame:
    """process_str_match_questions tworzy zmienne 0-1 z pytań z możliwością dodawania własnych opcji

    W funkcji wybierane są unikalne wartości wybierane przez użytkownika i dopasywane pod nie regexy. Na podstawie wstępnego OHE tworzone są ostateczne kolumny z wartościami binarnymi wskazującymi, które symptomy zostały wybrane.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych
    question_id : int
        id pytania (musi być typu multiselect)

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi wyborów użytkownika w danym pytaniu
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    try:
        pattern = str_match_questions_settings[question_id]["pattern"]
    except:
        print(f"Question id {question_id} not in settings regex!")
    list_of_classes = str_match_questions_settings[question_id]["list_of_classes"]

    if pattern is not None:
        response = (
            data.loc[data.id_question == question_id]
            .odpowiedzi.apply(lambda x: re.findall(pattern, x))
            .apply(lambda x: [y.upper().strip().split(",") for y in x])
            .apply(lambda x: list(chain.from_iterable(x)))
            .apply(lambda x: list(set([unidecode(y.strip()) for y in x])))
        )
    else:
        response = (
            data.loc[data.id_question == question_id]
            .odpowiedzi.str.upper()
            .str.strip()
            .fillna("NO")
            .apply(unidecode)
            .str.split(" ")
        )

    final_df = pd.DataFrame(index=response.index, columns=list_of_classes)
    if fillna:
        final_df.fillna(0, inplace=True)
    final_df = match_to_columns(final_df, response)
    final_df = final_df.add_prefix(prefix=prefix)
    final_df.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return final_df


# process_multiselect_questions
def process_multiselect_questions(data: pd.DataFrame, question_id: int) -> pd.DataFrame:
    """process_multiselect_questions tworzy zmienne 0-1 z pytań multiselect

    W funkcji wybierane są unikalne wartości wybierane przez użytkownika i dopasywane pod nie regexy. Na podstawie wstępnego OHE tworzone są ostateczne kolumny z wartościami binarnymi wskazującymi, które symptomy zostały wybrane.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych
    question_id : int
        id pytania (musi być typu multiselect)

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi wyborów użytkownika w danym pytaniu
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    try:
        response = (
            data.loc[data.id_question == question_id]
            .odpowiedzi.apply(lambda x: json.loads(x)["checkbox"])
            .explode()
        )
    except:
        response = (
            data.loc[data.id_question == question_id]
            .odpowiedzi.str.replace('"', "")
            .str.replace("[", "")
            .str.replace("]", "")
            .str.replace(" ", "")
            .str.split(",")
            .explode()
        )

    response = pd.get_dummies(response).groupby(response.index).sum()
    if "" in response.columns:
        response.drop(columns=[""], inplace=True)

    # unique_values = pd.get_dummies(
    #     pd.DataFrame(
    #         data.loc[data.id_question == question_id]
    #         .odpowiedzi.str.replace('"', "")
    #         .str.replace(" ", "")
    #         .str.replace("[", "")
    #         .str.replace("]", "")
    #         .str.split(",")
    #         .to_list()
    #     )
    # )
    # unique_values.drop(
    #     columns="0_", inplace=True
    # )  # W przypadku, gdy odpowiedzią jest '[]', czyli pusta lista

    # final_values = (
    #     unique_values.columns.str.replace("[0-9]+_", "", regex=True)
    #     .str.strip()
    #     .unique()
    #     .to_list()
    # )
    # values_regex = [".*" + x for x in final_values]
    # values_dict = dict(zip(final_values, values_regex))
    # values_dict["NO"] = "0_NO"

    # for column, regex in values_dict.items():
    #     columns_list = list(filter(re.compile(regex).match, unique_values.columns))
    #     unique_values[column] = unique_values[columns_list].sum(axis=1)
    # response = unique_values[values_dict.keys()]
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


# process_numeric_questions
def process_numeric_questions(
    data: pd.DataFrame,
    question_id: int,
    threshold: Dict[float, Dict[str, float]] = threshold,
) -> pd.Series:
    """process_numeric_question zamienia zmienną liczbową na float

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        ID pytania
    threshold : Dict[float, Dict[str, float]], optional
        Próg dolny i górny zmiennej, by default None

    Returns
    -------
    pd.Series
        Zmienna w formacie float
    """

    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    response = pd.to_numeric(
        data.loc[data.id_question ==
                 question_id].odpowiedzi.str.replace(",", "."),
        errors="coerce",
    ).rename(nazwa_zmiennej)
    if question_id in threshold.keys():
        response.loc[
            (response > threshold[question_id]["max"])
            | (response < threshold[question_id]["min"])
        ] = np.nan
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


# process_single_select_questions
def process_single_select_questions(
    data: pd.DataFrame, question_id: int
) -> pd.DataFrame:
    """process_single_select_questions tworzy n zmiennych binarnych na podstawie id pytania (OHE)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        ID pytania

    Returns
    -------
    pd.DataFrame
        Zmienna po OHE
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = data.loc[data.id_question ==
                        question_id].odpowiedzi.str.replace('"', "")
    response = pd.get_dummies(response, drop_first=False)
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    response = response.groupby(response.index).sum()
    return response


# process_binary_questions
def process_binary_questions(data: pd.DataFrame, question_id: int) -> pd.Series:
    """process_easy_dummy_question zamienia zmienną tekstową z 2 etykietami na zmienną binarną (tak/nie)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        ID pytania

    Returns
    -------
    pd.Series
        Zmienna binarna
    """
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    response = data.loc[data.id_question ==
                        question_id].odpowiedzi.str.replace('"', "")
    missing_data = (response == "DONT_KNOW") | (
        response == "nie wiem") | (response == "DONT_REMEMBER")
    response = (
        ((response != "NO") & (response != "{radio: NO}"))
        .astype(int)
        .rename(nazwa_zmiennej)
    )
    response.loc[missing_data] = np.nan
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_genetic_questions(data: pd.DataFrame, question_id: int) -> pd.DataFrame:
    """process_genetic_questions zamienia pytania z sekcji genetycznej na tabelę, gdzie kolumny są odpowiedziami a wartości listą osób zaznaczoną przez użytkownika

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        ID pytania

    Returns
    -------
    pd.DataFrame
        Zmienna binarna
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.str.replace('{"{', "{")
        .str.replace('}"}', "}")
        .str.replace("\\", "")
        .apply(lambda x: json.loads(x) if x not in ["NO", "YES"] else [x])
        .apply(pd.Series)
    )
    response.drop(columns=[0], inplace=True)
    for column_name in response:
        response[column_name] = response[column_name].apply(
            lambda x: int(np.isin("ME", x)  # | (len(x) > 1)
                          ) if isinstance(x, list) else 0
        )
    response = response.add_prefix(prefix=prefix)
    response.drop(
        columns=list(set(response.columns.tolist()) & set(drop_genetic_cols)),
        inplace=True,
    )
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response

# process_date_questions


def process_date_questions(data: pd.DataFrame, question_id: int) -> pd.DataFrame:
    """process_date_questions przeprocesowuje pytania z sekcji date_questions_ i oblicza ile miesięcy minęło od daty odpowiedzi w ankiecie,
    a datą ostatniego badania (cytologia, mammografia i USG piersie, wizyta u ginekologa, USG ginekologiczne).

    Parameters
    ----------
    data : pd.DataFrame
        zbiór danych wejściowych
    question_id : int
        id pytania z sekcji date question

    Returns
    -------
    pd.DataFrame
        Funkcja zwraca tabelę, gdzie odpowiedzią na pytanie jest ile miesięcy minęło od odpowiedzi w ankiecie, a datą badania.
    """

    response = data.loc[data.id_question == question_id].odpowiedzi
    response = response.str.extract(
        r"(\d*-\d*-\d*).\d*:\d*:\d*.\d*.", expand=True)

    response.columns = [
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
        + "_DATE_VAR"
    ]

    for col in response:
        response[col] = pd.to_datetime(
            response[col], errors="coerce", utc=True)

    response_date = data.loc[:, ["wizyta_id", "response_date"]]

    response = response.merge(
        response_date, how="left", left_index=True, right_index=True)

    response["response_date"] = pd.to_datetime(
        response["response_date"], errors="coerce", utc=True
    )

    response[
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
        + "_DATE_VAR"
    ] = (
        (
            response["response_date"]
            - response[
                str(question_id)
                + "_"
                + data.loc[data.id_question == question_id]
                .pytanie_pl.iloc[0]
                .replace(" ", "_")
                + "_DATE_VAR"
            ]
        )
        / np.timedelta64(1, "M")
    ).round(
        1
    )

    response.drop(["wizyta_id", "response_date"], axis=1, inplace=True)

    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response

# process_map_questions


def process_map_questions(data: pd.DataFrame, question_id=id) -> pd.Series:
    """process_map_questions _summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        _description_
    question_id : int
        _description_

    Returns
    -------
    pd.Series
        _description_
    """
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    mapper = {
        '"DONOR_ONE"': 1,
        '"DONOR_TWO"': 2,
        '"DONOR_THREE"': 3,
        '"DONOR_FOUR"': 4,
        '"DONOR_FIVE"': 5,
        '"DONOR_SIXANDMORE"': 6,
        '"DONOR_SEVEN"': 7,
        '"DONOR_EIGHT"': 8,
        '"DONOR_NINE"': 9,
        '"DONOR_TEN"': 10,
        '"DONOR_ELEVEN"': 11,
        '"DONOR_TWELVE"': 12,
        '"DONOR_THIRTEEN"': 13,
        '"DONOR_FOURTEEN"': 14,
        '"DONOR_FIVETEEN"': 15,
        '"DONOR_SIXTEEN"': 16,
        '"DONOR_SEVENTEEN"': 17,
        '"DONOR_EIGHTEENANDLESS"': 18,
        '"DONOR_NINETEEN"': 19,
        '"DONOR_TWENTY"': 20,
        '"DONOR_TWENTYONE"': 21,
        '"DONOR_TWENTYTWO"': 22,
        '"DONOR_TWENTYTHREE"': 23,
        '"DONOR_TWENTYFOUR"': 24,
        '"DONOR_TWENTYFIVE"': 25,
        '"DONOR_TWENTYSIX"': 26,
        '"DONOR_TWENTYSEVEN"': 27,
        '"DONOR_TWENTYEIGHT"': 28,
        '"DONOR_TWENTYNINE"': 29,
        '"DONOR_THIRTY"': 30,
        '"DONOR_THIRTYONE"': 31,
        '"DONOR_THIRTYTWO"': 32,
        '"DONOR_THIRTYTHREE"': 33,
        '"DONOR_THIRTYFOUR"': 34,
        '"DONOR_THIRTYFIVEANDMORE"': 35,
    }
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.map(mapper)
        .rename(nazwa_zmiennej)
    )
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def unpact_dict(value: List) -> Dict:
    """unpact_dict _summary_

    _extended_summary_

    Parameters
    ----------
    value : List
        _description_

    Returns
    -------
    Dict
        _description_
    """
    mapper = {
        "DONOR_ONE": 1,
        "DONOR_TWO": 2,
        "DONOR_THREE": 3,
        "DONOR_FOUR": 4,
        "DONOR_FIVE": 5,
        "DONOR_SIX": 6,
        "DONOR_SEVEN": 7,
        "DONOR_EIGHT": 8,
        "DONOR_NINE": 9,
    }
    year = value[0]
    try:
        type_misscarriage = value[1]["radio"]  # 222
    except:
        type_misscarriage = value[1]
    how_long_pregnant = value[2]
    if how_long_pregnant in mapper.keys():
        how_long_pregnant = mapper[how_long_pregnant]
    current_partner = value[3]
    return year, type_misscarriage, how_long_pregnant, current_partner


def del_stupid_keys(x: Dict, min_key: int = 12) -> Dict:
    """del_stupid_keys _summary_

    _extended_summary_

    Parameters
    ----------
    x : Dict
        _description_
    min_key : int, optional
        _description_, by default 12

    Returns
    -------
    Dict
        _description_
    """
    max_key = len(x.keys())
    if max_key > min_key:
        for i in range(min_key, max_key):
            del x[str(i)]
    len_keys = len(x.keys())
    response = {}
    misscarriage = []
    born = []
    misscarriage_current_partner = []
    when_misscarriage = []
    when_born = []
    pregnancy_duration = []
    relevant_key = False
    if len_keys > 0:
        for i in range(len_keys):
            if sum(pd.isna(x[str(i)])) == 5:
                del x[str(i)]
            else:
                relevant_key = True
                (
                    year,
                    type_misscarriage,
                    how_long_pregnant,
                    current_partner,
                ) = unpact_dict(x[str(i)])
                is_misscarriaged = type_misscarriage != "CHILDBIRTH"
                if is_misscarriaged:
                    misscarriage.append(is_misscarriaged)
                    when_misscarriage.append(year)
                    pregnancy_duration.append(how_long_pregnant)
                    misscarriage_current_partner.append(current_partner)
                else:
                    when_born.append(year)
                    born.append("CHILDBIRTH")
        misscarriage = [x for x in misscarriage if x]
        born = [x for x in born if x]
        misscarriage_current_partner = [
            x for x in misscarriage_current_partner if x]
        when_misscarriage = [x for x in when_misscarriage if x]
        when_born = [x for x in when_born if x]
        pregnancy_duration = [x for x in pregnancy_duration if x]
        if relevant_key:
            response["liczba_poronien"] = len(misscarriage)
            response["liczba_ciaz"] = len(born)
            response["liczba_poronien_obecny_partner"] = len(
                misscarriage_current_partner
            )
            # response["najwczesniejsze_poronienie"] = min(
            #     when_misscarriage, default=np.nan
            # )
            # response["najpozniejsze_poronienie"] = max(
            #     when_misscarriage, default=np.nan
            # )
            # response["najwczesniejsza_ciaza"] = min(when_born, default=np.nan)
            # response["najpozniejsza_ciaza"] = max(when_born, default=np.nan)
            # response["mediana_dlugosci_poronien"] = np.median(
            #     pregnancy_duration)
    return response


# process_pregnancy_questions
def process_pregnancy_questions(data: pd.DataFrame, question_id: int, fillna:bool = True) -> pd.DataFrame:
    """
    Przygotowanie pytaniao liczbę ciąż oraz poronień

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        id pytania

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami liczbowymi. Dostępne kolumny : 'liczba_poronien', 'liczba_ciaz', 'liczba_poronien_obecny_partner', 'najwczesniejsze_poronienie', 'najpozniejsze_poronienie', 'najwczesniejsza_ciaza', 'najpozniejsza_ciaza', 'mediana_dlugosci_ciazy'
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.map(json.loads)
        .apply(pd.Series)
    )
    response = response.question2nd.apply(del_stupid_keys)
    response = response.apply(pd.Series)
    response = response.add_prefix(prefix=prefix)
    if fillna:
        response.fillna(0, inplace=True)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response

# process binary_multiselect questions


def process_binary_multiselect_questions(
    data: pd.DataFrame, question_id: int
) -> pd.DataFrame:
    """process_binary_multiselect_questions przeprocesowuje pytania z sekcji binary_multiselect DETAIL_INTERVIEW - zwraca kolumny jako tabelę binarną oraz tabelę z odpowiedziami multiselect.
    Przygotowanie to jest potrzebne do funkcji mapującej te same odpowiedzi do różnych pytań. Kolumny z odpowiedziami binarnymi mają prefix BINARY_ tak by w kolejnym kroku mapowania nie zostały
    usunięte (gdyż mapowanie odbywać się będzie na postawie ID przypisanych jako prefix). Z pytań multiselect usunięte są równieź odpowiedzi YES NO gdyż pokrywa te odpowiedz wersja binarna pytań.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych do przeprocesowania
    question_id : int
        id pytania

    Returns
    -------
    pd.DataFrame
        Zmergowana tabela binarna oraz tabela z odpowiedziami typu multiselect.
    """
    nazwa_zmiennej = (
        "BINARY_"
        + str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    data_binary = data.loc[data.id_question == question_id].odpowiedzi.str.replace(
        '"', ""
    )
    missing_data = (
        (data_binary == "DONT_KNOW")
        | (data_binary == "nie wiem")
        | (data_binary == "DONT_REMEMBER")
    )
    data_binary = (
        ((data_binary != "NO") & (data_binary != "{radio: NO}"))
        .astype(int)
        .rename(nazwa_zmiennej)
    )
    data_binary.loc[missing_data] = np.nan
    data_binary = pd.DataFrame(data_binary)

    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    data_multiselect = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.str.replace('"', "")
        .str.replace("[", "")
        .str.replace("]", "")
        .str.replace(" ", "")
        .str.replace("DONT_REMEMBER", "")
        .str.split(",")
        .explode()
    )

    data_multiselect = (
        pd.get_dummies(data_multiselect).groupby(data_multiselect.index).sum()
    )
    if "" in data_multiselect.columns:
        data_multiselect.drop(columns=[""], inplace=True)

    data_multiselect = data_multiselect.add_prefix(prefix=prefix)

    patterns = [":YES", ":NO"]

    for pattern in patterns:

        data_multiselect.drop(
            (data_multiselect.filter(regex=pattern).columns), axis=1, inplace=True
        )

    response = pd.merge(
        data_binary, data_multiselect, how="left", left_index=True, right_index=True
    )
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response
# process_distinct_questions


def process_question_139(data: pd.DataFrame, question_id: int = 139) -> pd.DataFrame:
    """
    Przygotowanie pytania o przebyte choroby. Jeden wpis w tabeli oznacza jeden checkbox, jednak pacjent może zaznaczyć więcej niż 1 opcję - dlatego musimy grupować.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 139

    Returns
    -------
    pd.DataFrame
        Wartości binarne
    """
    response = (
        pd.merge(
            left=data["wizyta_id"],
            right=process_single_select_questions(data, 139),
            left_index=True,
            right_index=True,
        )
        .groupby("wizyta_id")
        .sum()
    )
    return response


def process_question_223(data: pd.DataFrame, question_id: int = 223) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 223

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami liczbowymi
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    slice = data.loc[data.id_question == question_id].odpowiedzi
    response = (
        (
            slice.apply(lambda x: json.loads(x)["checkbox"])
            + slice.apply(lambda x: list(json.loads(x)["input"].keys()))
        )
        .apply(lambda x: list(set(x)))
        .explode()
    )
    response = pd.get_dummies(response).groupby(response.index).sum()
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_119(data: pd.DataFrame, question_id: int = 119) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 119

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami liczbowymi
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    slice = data.loc[data.id_question == question_id].odpowiedzi
    response = (
        (
            slice.apply(lambda x: json.loads(x)["checkbox"])
            + slice.apply(lambda x: list(json.loads(x)["input"].keys()))
        )
        .apply(lambda x: list(set(x)))
        .explode()
    )
    response = pd.get_dummies(response).groupby(response.index).sum()
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_64(data: pd.DataFrame, question_id: int = 64) -> pd.DataFrame:
    """
    Przygotowanie pytania o liczbę biologicznych bombelków.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 64

    Returns
    -------
    pd.DataFrame
        Kolumny zawierające informacje o dzieciach/z obecnym partnerem
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = data.loc[data.id_question == question_id].odpowiedzi.apply(
        lambda x: [re.sub("[^0-9]", "", y) for y in x.split("},{")]
    )
    response = pd.DataFrame(
        response.tolist(),
        columns=[
            "Liczba biologicznych dzieci",
            "Liczba biologicznych dzieci z obecnym partnerem",
        ],
    )
    response.replace("", 0, inplace=True)
    response = response.add_prefix(prefix=prefix)
    response = response.astype(float)
    for col in response:
        if question_id in threshold.keys():
            response.loc[
                (response[col] > threshold[question_id]["max"])] = np.nan
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_356(data: pd.DataFrame, question_id: int = 356) -> pd.DataFrame:
    """
    Przygotowanie pytania o wykonywany zawód.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 356

    Returns
    -------
    pd.DataFrame
        Kolumny zawierające informacje o wykonywanym zawodzie
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = pd.Series(
        [
            y[0]
            for y in data.loc[data.id_question == question_id].odpowiedzi.str.split(";")
        ],
        name="Wykonywany zawod",
    )
    response = pd.get_dummies(response, drop_first=False)
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_439(data: pd.DataFrame, question_id: int = 439) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne (jedno z 2).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 439

    Returns
    -------
    pd.DataFrame
        Zmienna zawierająca informację o stosowanych środkach antykoncepcyjnych
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.str.replace('{"{', "{")
        .str.replace('}"}', "}")
        .str.replace("\\", "")
        .apply(lambda x: list(json.loads(x).keys()) if x not in ["NO", "YES"] else [x])
        .explode()
    )
    response = pd.get_dummies(response).groupby(response.index).sum()
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_151(data: pd.DataFrame, question_id: int = 151) -> pd.DataFrame:
    """
    Przygotowanie pytania o palenie papierosów.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 151

    Returns
    -------
    pd.DataFrame
        Tabela zawierająca 3 kolumny - ile papierosów palisz dziennie, ile lat palisz, czy rzuciłaś
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.str.replace('"', "")
        .str.replace("[", "")
        .str.replace("]", "")
        .str.replace(" ", "")
        .str.split(",")
    )
    ile_dziennie = response.apply(lambda x: x[0] if len(x) > 1 else np.nan).rename(
        "ile_dziennie"
    )
    ile_dziennie = pd.to_numeric(ile_dziennie, errors="coerce")
    # ile_lat = (
    #     response.apply(lambda x: x[1] if len(x) > 1 else np.nan)
    #     .rename("ile_lat")
    #     .astype(float)
    # )
    rzucila_palenie = response.apply(
        lambda x: int(x[2] != "null") if len(x) > 1 else np.nan
    ).rename("rzucila_palenie")
    rzucila_palenie = pd.to_numeric(rzucila_palenie, errors="coerce")
    response = pd.DataFrame(
        [
            ile_dziennie,
            # ile_lat,
            rzucila_palenie,
        ]
    ).T
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_470(data: pd.DataFrame, question_id: int = 470) -> pd.DataFrame:
    """
    Przygotowanie pytania o palenie papierosów.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 470

    Returns
    -------
    pd.DataFrame
        Tabela zawierająca 3 kolumny - ile papierosów palisz dziennie, ile lat palisz, czy rzuciłaś
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.str.replace('"', "")
        .str.replace("[", "")
        .str.replace("]", "")
        .str.replace(" ", "")
        .str.split(",")
    )
    ile_dziennie = response.apply(lambda x: x[0] if len(x) > 1 else np.nan).rename(
        "ile_dziennie"
    )
    ile_lat = response.apply(lambda x: x[1] if len(
        x) > 1 else np.nan).rename("ile_lat")
    rzucila_palenie = response.apply(
        lambda x: x[2] != "null" if len(x) > 1 else np.nan
    ).rename("rzucila_palenie")
    rzucila_palenie = pd.to_numeric(rzucila_palenie, errors="coerce")
    # ile_lat,
    response = pd.DataFrame([ile_dziennie, rzucila_palenie]).T
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_110(data: pd.DataFrame, question_id: int = 110) -> pd.DataFrame:
    """Przygotowanie pytania: czy przyjmowała/przyjmuje Pani jakiekolwiek leki (przyjmowane leki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 110

    Returns
    -------
    pd.DataFrame
        Tabela w wartościami binarnymi dla każdego leku
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    # response = (
    #     data.loc[data.id_question == question_id]
    #     .odpowiedzi.apply(
    #         lambda x: list(json.loads(x).keys()) if x not in [
    #             "NO", "YES"] else [x]
    #     )
    #     .explode()
    # )
    # response = pd.get_dummies(response).groupby(response.index).sum()
    response = data.loc[data.id_question == question_id].odpowiedzi
    response = (response.apply(lambda x: json.loads(
        x) if x not in ["NO", "YES"] else [x])).apply(pd.Series)
    response = response.explode(response.columns.drop(0).tolist())
    response.drop(columns=0, inplace=True)
    response = response[~response.index.duplicated(keep="first")].fillna(
        value="NO").replace({"TRUE": 1, "FALSE": 0, "NO": 0, "YES": 1})
    response = response.replace(to_replace=r'.+', value=1, regex=True)
    response = response.replace('', 0)
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response


def process_question_108(data: pd.DataFrame, question_id: int = 108) -> pd.DataFrame:
    """Przygotowanie pytania: czy w trakcie dotychczasowej diagnostyki miała Pani przeprowadzane
    jakiekolwiek procedury medyczne np. udrożnienie jajników? (rodzaj diagnostyki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 108

    Returns
    -------
    pd.DataFrame
        Tabela w wartościami binarnymi dla każdej diagnostyki
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    cols_to_explode = ['ET',
                       'AID',                            'ETM',
                       'IUI',                            'IVF',
                       'PGD',                           'TESE',
                       'IVF_ICSI',                  'EGG_RECEPTION',
                       'SEMEN_RECEPTION',              'EMBRYOS_RECEPTION',
                       'REMOVAL_PREGNANCY',           'SURGICAL_LAPAROSCOPY',
                       'REMOVAL_OVARIAN_CYSTS',          'SURGICAL_HYSTEROSCOPY',
                       'DRAINAGE_FALLOPIAN_TUBES', 'REMOVAL_VARICOSE_VEINS_PARTNER']

    response = data.loc[data.id_question == question_id].odpowiedzi
    response = (response.apply(lambda x: json.loads(
        x) if x not in ["NO", "YES"] else [x])).apply(pd.Series)
    response = response.explode(cols_to_explode)
    response.drop(columns=0, inplace=True)
    response = response[~response.index.duplicated(keep="first")].fillna(
        value="NO").replace({"TRUE": 1, "FALSE": 0, "NO": 0, "YES": 1})

    response = response.add_prefix(prefix=prefix)

    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_86(data: pd.DataFrame, question_id: int = 86) -> pd.DataFrame:
    """Przygotowanie pytania o grupę krwi. Ujednolicenie formatu odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zespół danych wejściowych
    question_id : int, optional
        id pytania, by default 86

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla każdej grupy krwi
    """

    response = process_single_select_questions(data, 86)
    response.columns = response.columns.str.replace("_GROUP", "_TYPE")

    return response


def process_question_88(data: pd.DataFrame, question_id: int = 88) -> pd.DataFrame:
    """Przygotowanie pytania o czynnik Rh. Ujednolicenie formatu odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zespół danych wejściowych
    question_id : int, optional
        id pytania, by default 139

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla pytania o czynnik Rh
    """

    response = process_single_select_questions(data, 88)
    response.columns = response.columns.str.replace("_FACTORY", "_RH_FACTOR")

    return response


def process_question_92(data: pd.DataFrame, question_id: int = 92) -> pd.DataFrame:
    """Przygotowanie pytania: wzrost

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 92
    Returns
    -------
    pd.DataFrame
        Tabela z wartościami numerycznymi jako wzrost pacjentek w cm
    """
    response = data.loc[data.id_question == question_id].odpowiedzi.apply(
        lambda x: re.sub(r"\d(.)\d\d", "", x)
    )

    nazwa_zmiennej = data.loc[data.id_question ==
                              question_id].pytanie_pl.iloc[0]
    response = pd.to_numeric(
        data.loc[data.id_question ==
                 question_id].odpowiedzi.str.replace(",", "."),
        errors="coerce",
    ).rename("92_" + nazwa_zmiennej)
    if question_id in threshold.keys():
        response.loc[
            (response > threshold[question_id]["max"])
            | (response < threshold[question_id]["min"])
        ] = np.nan

    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response


def process_question_428(data: pd.DataFrame, question_id: int = 428) -> pd.DataFrame:
    """process_question_428 mapuje odpowiedzi na pytanie: liczba nieregularnych cykli w ciągu roku na odpowiedzi binarne 1 - cykl nieregularny, 0 - cykl regularny. 
    Założony treshold do 4 cykli nieregularnych w ciągu roku klasyfikowany jako cykl regularny.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 428

    Returns
    -------
    pd.DataFrame
        Zmienna binarna
    """
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    mapping = {
        "DONOR_ONE": 0,
        "DONOR_TWO": 0,
        "DONOR_THREE": 0,
        "DONOR_FOUR": 0,
        "DONOR_FIVE": 1,
        "DONOR_SIX": 1,
        "DONOR_SEVEN": 1,
        "DONOR_EIGHT": 1,
        "DONOR_NINE": 1,
        "DONOR_TEN": 1,
        "DONOR_ELEVEN": 1,
        "DONOR_TWELVE": 1,
        "YES": 1,
        "NO": 0,
    }

    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.map(mapping)
        .rename(nazwa_zmiennej)
    )

    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response


def process_question_226(data: pd.DataFrame, question_id: int = 226) -> pd.DataFrame:
    """Przygotowanie pytania: czy przyjmowała/przyjmuje Pani jakiekolwiek leki (przyjmowane leki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 226

    Returns
    -------
    pd.DataFrame
        Tabela w wartościami binarnymi dla każdego leku
    """
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    # response = (
    #     data.loc[data.id_question == question_id]
    #     .odpowiedzi.apply(
    #         lambda x: list(json.loads(x).keys()) if x not in [
    #             "NO", "YES"] else [x]
    #     )
    #     .explode()
    # )
    # response = pd.get_dummies(response).groupby(response.index).sum()

    response = data.loc[data.id_question == question_id].odpowiedzi
    response = (response.apply(lambda x: json.loads(
        x) if x not in ["NO", "YES"] else [x])).apply(pd.Series)
    response = response.explode(response.columns.drop(0).tolist())
    response.drop(columns=0, inplace=True)
    response = response[~response.index.duplicated(keep="first")].fillna(
        value="NO").replace({"TRUE": 1, "FALSE": 0, "NO": 0, "YES": 1})
    response = response.replace(to_replace=r'.+', value=1, regex=True)
    response = response.replace('', 0)
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response


def process_order_questions(
    data: pd.DataFrame, question_id: int, mapping_order_scale: Dict = mapping_order_scale
) -> pd.DataFrame:
    """process_single_select_questions tworzy zmienną na podstawie mapowania

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int
        ID pytania

    Returns
    -------
    pd.DataFrame
        Zmienna po single select i mapowaniu
    """
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    response = data.loc[data.id_question ==
                        question_id].odpowiedzi.str.replace('"', "").map(
        mapping_order_scale
    ).astype(int).rename(nazwa_zmiennej)
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    response = response.groupby(response.index).max()
    return response


order_questions = [
    102,  # czas starania się o dziecko
    104,  # częstotliwość stosunków
    213,  # dolegliwości bólowe podczas okresu
    214  # krwawienie
]


def process_question_427(data: pd.DataFrame, question_id: int = 427) -> pd.DataFrame:
    """process_question_427 procesuje pytanie: W jakim wieku miała Pani pierwszą miesiączkę? Zostało zastosowane mapowanie, gdzie early menarche < 10lat, normal menarche 10-16lat, late menarche > 16lat.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 427

    Returns
    -------
    pd.DataFrame
        Tabela binarna
    """
    # prefix = (
    #     str(427)
    #     + "_"
    #     + data.loc[data.id_question == question_id]
    #     .pytanie_pl.iloc[0]
    #     .replace(" ", "_")
    #     .replace(":", "")
    #     + ":"
    # )
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question ==
                   question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )

    menarche_mapping = {
        '"DONOR_BEFORENINE"':0,# '"EARLY_MENARCHE"',
        '"DONOR_TEN"': 0, #'"NORMAL_MENARCHE"',
        '"DONOR_ELEVEN"': 1, #'"NORMAL_MENARCHE"',
        '"DONOR_TWELVE"': 1, #'"NORMAL_MENARCHE"',
        '"DONOR_THIRTEEN"': 1, #'"NORMAL_MENARCHE"',
        '"DONOR_FOURTEEN"': 2, #'"NORMAL_MENARCHE"',
        '"DONOR_FIFTEEN"': 2, #'"NORMAL_MENARCHE"',
        '"DONOR_SIXTEEN"': 3, #'"LATE_MENARCHE"',
        '"DONOR_AFTERSEVENTEEN"': 3, #'"LATE_MENARCHE"',
    }

    response = data.loc[data.id_question == question_id].odpowiedzi.map(
        menarche_mapping
    )
    # response = pd.get_dummies(response)

    # response = response.add_prefix(prefix=prefix)
    # response.set_index(
    #     data.loc[data.id_question == question_id].wizyta_id, inplace=True
    # )
    response = response.rename(nazwa_zmiennej)
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    response = response.groupby(response.index).max()

    
    return response

# Mapowanie pytan


duplicates_numeric = {
    "Średnia długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)": [
        209,
        429,
    ],
    "Najkrótsza długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)": [
        210,
        430,
    ],
    "Najdłuższa długość cyklu miesiączkowego (liczba dni od pierwszego dnia miesiączki do pierwszego dnia następnej miesiączki)": [
        211,
        431,
    ],
}
duplicates_binary = {
    "Czy kiedykolwiek rozpoznano u Pani niepłodność?": [114, 437, 445],
    "Czy kiedykolwiek stosowała Pani narkotyki?": [153, 472],
    "Czy pije Pani alkohol?": [150, 644],
    "Czy występują w Pani rodzinie jakieś znane Pani choroby genetyczne lub wady wrodzone?": [
        198,
        199,
        515,
        521,
    ],
    "Czy choruje/chorowała Pani na chorobę nowotworową?": [557, 559, 561],
    "Czy w Pani rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?": [
        529,
        533,
        535,
        547,
    ],
    "Czy w Pani rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?": [
        527,
        541,
        567,
    ],
    "Czy była/jest Pani narażona na działanie związków mutagennych?": [572, 573],
    "Czy miesiączkuje Pani nieregularnie?": [220, 428],
    "Czy była Pani adoptowana?": [196, 474]
}
duplicates_single_select = {
    "Grupa krwi": [86, 617],
    "Czynnik Rh": [88, 619],
    # "Zawód wykonywany": [76, 356],
    "Czy_aktualnie_się_Pani_leczy?": [159, 240, 462, 129],
    "Czy_stosuje_Pani_środki_antykoncepcyjne?": [655, 594, 119, 223, 439],
    "Ile_razy_była_Pani_w_ciąży?": [121, 222, 442],
    "Czy przyjmuje Pani obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)": [
        110,
        123,
        226,
        491
    ],
    "Czy pali/paliła Pani papierosy?": [151, 470],
    "Proszę zaznaczyć szczepienia, które Pani kiedykolwiek miała": [143, 497],
    # "Jak_często_odbywa_Pani_stosunki_płciowe?": [104]
}
mapping_for_duplicates = {
    # "76_356": {
    #     "EXPERT": "MANAGEMENT",
    #     "MANAGER": "MANAGEMENT",
    #     "RESEARCH_WORKER": "OFFICE_WORKER",
    # },
    # "104": {
    #     "ONE_OR_SEVERAL_TIMES_DAY": "SEVERAL_TIMES_DAY"
    # },
    "159_240_462_129": {
        "POLICYSTYCZNYCH": "PCOS",
        "TARCZYCA": "NIEDOCZYNNOSC",
    },
    "655_594_119_223_439": {
        "ABSTINENCE": "NO",
        "NONE": "NO",
        "FOAM_GEL": "SPERMICIDE",
        "INTERMITTENT": "NO",
        "LIGATION_OF_FALLOPIAN_TUBES": "PERMANENT_CONTRACEPTION",
        "LIGATON_OF_EP": "PERMANENT_CONTRACEPTION",
        "CALENDAR": "NO",
        "DIAPHRAGM": "CONDOM",
    },
    "110_123_226_491": {
        "ACARD": "ACETYLOSALICYLIC_ACID",
        "ACESAN": "ACETYLOSALICYLIC_ACID",
        "ASENTRA": "SERTRALINE",
        "ASERTIN": "SERTRALINE",
        "BELARA": "HORMONAL_CONTRACEPTIVE",
        "BROMERGON": "BROMOCRIPTINE",
        "CIPRONEX": "CIPROFLOXACIN",
        "CLEXANE": "HEPARIN",
        "CLOSTILBEGYT": "CLOMIFEN",
        "D3": "D",
        "DECAPEPTYL": "TRIPTORELIN",
        "DIPHERELINE": "TRIPTORELIN",
        "DOPEGYT": "METHYLDOPA",
        "DOSTINEX": "CABERGOLINE",
        "DUPHASTON": "DYDROGESTERONE",
        "ENCORTON": "PREDNISONE",
        "ESTROFEM": "ESTRADIOL",
        "EUTHYROX": "LEVOTHYROXINE",
        "FEMIBION": "FOLIC_ACID",
        "FERTISTIM": "FOLIC_ACID",
        "FOLIK": "FOLIC_ACID",
        "FOLIOWY": "FOLIC_ACID",
        "FRAXIPARYNA": "NADROPARIN",
        "FURAGINA": "FURAZIDINE",
        "GLUCOPHAGE": "METFORMIN",
        "INOFEM": "FOLIC_ACID",
        "LAMETTA": "LETROZOLE",
        "LETROX": "LEVOTHYROXINE",
        "LUTEINA": "PROGESTERONE",
        "MAGNEZ": "MAGNESIUM",
        "MENOGON": "MENOTROPIN",
        "MENOPUR": "MENOTROPIN",
        "METFORMAX": "METFORMIN",
        "MIOVELIA": "FOLIC_ACID",
        "NEOPARIN": "HEPARIN",
        "OVARIN": "FOLIC_ACID",
        "OVITRELLE": "CHORIOGONADOTROPIN",
        "PARLODEL": "BROMOCRIPTINE",
        "PREGNA": "FOLIC_ACID",
        "PRENATAL": "FOLIC_ACID",
        "PROVERA": "MEDROXYPROGESTERON",
        "PUREGON": "FSH",
        "SIOFOR": "METFORMIN",
        "TARDYFERON": "IRON",
        "TRITTICO": "TRAZODONE",
        "VIBIN": "HORMONAL_CONTRACEPTIVE",
        "ZOLOFT": "SERTRALINE",
    },
}

multiselect_mapping = {
    "DIFFICULTY_FALLING_ASLEEP": "SLEEP_DISORDERS",
    "SKIN_ALLERGIES": "PERSISTENT_RASH",
    "FEVER_SKIN_RASH": "PERSISTENT_RASH",
    "PROLONGED_OR_PERSISTENT_ITCHING": "PERSISTENT_RASH",
    "PSORIASIS": "PERSISTENT_RASH",
    "UNEXPLAINED_FEVER": "FEVER",
    "HAY_FEVER": "FEVER",
    "RHEUMATIC_FEVER": "FEVER",
    "WEIGHT_LOSS_MORE_OF_5KG": "UNEXPLAINED_WEIGHT_LOSS",
    "ASTIGMATISM": "VISION_PROBLEMS",
    "BLURRED_VISION": "VISION_PROBLEMS",
    "CATARACT": "VISION_PROBLEMS",
    "GLAUCOMA": "VISION_PROBLEMS",
    "HYPEROPIA": "VISION_PROBLEMS",
    "MYOPIA": "VISION_PROBLEMS",
    "CHRONIC_EAR_PROBLEMS": "EAR_PROBLEMS",
    "TINNITUS": "EAR_PROBLEMS",
    "LOSS_HEARING": "EAR_PROBLEMS",
    "ABILITY_TO_NOSEBLEED": "RECURRENT_BLEEDING_NOSE",
    "CHEST_PAIN_PLEURISY": "CHEST_PAIN",
    "LOSS_OF_CONSCIOUSNESS": "FAINTING",
    "HEART_ARYTHMIA": "HEARTH_DISEASES",
    "HEART_DEFECTS": "HEARTH_DISEASES",
    "IRREGULAR_HEARTBEAT": "HEARTH_DISEASES",
    "MYOCARDITIS": "HEARTH_DISEASES",
    "MURED_IN_THE_HEART": "HEARTH_DISEASES",
    "LARGE_VARICOSE_VEINS_DEEP": "LARGE_VARICOSE_VEINS",
    "LONG_LASTING_WOUNDS": "BLEEDING_BRUISING",
    "POOR_BLOOD_SUPPLY": "LOW_BLOOD_PRESSURE",
    "POOR_CIRCULATION": "LOW_BLOOD_PRESSURE",
    "DIFFICULTY_IN_WALKING": "PAINS_ON_WALKING",
    "SHORTNESS_OF_BREATH_CAUSES_AWAKENING": "SHORTNESS_OF_BREATH",
    "DIFFICULTY_BREATHING_IN_THE_LYING_POSITION": "SHORTNESS_OF_BREATH",
    "WHISTLING_BREATH": "SHORTNESS_OF_BREATH",
    "SUSCEPTIBILITY_BRUISING_SWELLING": "BLEEDING_BRUISING",
    "BRONCHITIS": "LUNG_DISEASE",
    "PNEUMONIA": "LUNG_DISEASE",
    "DYSPNEA": "LUNG_DISEASE",
    "EMPHYSEMA": "LUNG_DISEASE",
    "LUNG_FAILURE": "ASTHMA",
    "ASTHMA": "LUNG_DISEASE",
    "BLOODY_COUGH": "PERSISTENT_COUGH",
    "DYSOAREUNIA": "REDUCTION_OF_SEX_DRIVE",
    "SEXUAL_PROBLEMS": "REDUCTION_OF_SEX_DRIVE",
    "WAKING_UP_TO_URINATE": "FREQUENT_URINATION",
    "WAKING_UP_TO_URINATE": "FREQUENT_URINATION",
    "SWELLING_OF_JOINTS": "RHEUMATIC_DISEASES",
    "ARTHRITIS": "RHEUMATIC_DISEASES",
    "GOUT": "RHEUMATIC_DISEASES",
    "UNEXPLAINED_WEAKNESS_OF_LOWER_LIMBS": "WEAKNESS",
    "WEAKENING_OF_MUSCLE_STRENGTH": "WEAKNESS",
    "MYASTHENIA_GRAVIS": "WEAKNESS",
    "UNEXPLAINED_FATIGUE": "WEAKNESS",
    "ECZEMA": "SEVERE_ACNE",
    "ATOPIC_LESIONS_OF_THE_SKIN": "SEVERE_ACNE",
    "EXCESSIVE_FACIAL_HAIR_OR_BODY": "EXCESSIVE_HAIR",
    "EXCESSIVE_FACIAL_BODY_HAIR": "EXCESSIVE_HAIR",
    "DIFFICULTIES_IN_MEMORIZATION": "THINKING_DISORDERS",
    "THINKING_DISORDERS": "THINKING_DISORDERS",
    "MIGRAINE": "HEADACHES",
    "PARESIS": "NUMBNESS",
    "COLD_INTOLERANCE": "HEAT_INTOLERANCE",
    "HOT_FLUSHES": "HEAT_INTOLERANCE",
    "PREGNANCY_DIABETES": "INSULIN_RESISTANCE",
    "HYPERTHYROIDISM": "THYROID_PROBLEMS",
    "THYROID_ENLARGEMENT": "THYROID_PROBLEMS",
    "HYPOTHYROIDISM": "THYROID_PROBLEMS",
    "CURRENT_LYMPH_NODE_ENLARGEMENT": "LYMPH_NODE_ENLARGEMENT",
    "LYMPH_NODE_ENLARGEMENT_MORE_THAN_MONTH": "LYMPH_NODE_ENLARGEMENT",
    "FEVER_ENLARGED_LYMPH_NODES": "LYMPH_NODE_ENLARGEMENT",
    "INCREASED_BLEEDING_AFTER_REMOVAL_TOOTH": "BLEEDING_BRUISING",
    "TENDENCY_TO_RUPTURE": "BLEEDING_BRUISING",
    "EASY_BRUISING": "BLEEDING_BRUISING",
    "UNEXPLAINED_HOARSENESS": "PERSISTENT_SORE_THROAT",
    "HEARTBURN": "PERSISTENT_SORE_THROAT",
    "PERSISTENT_WHITE_SPOTS_IN_MOUTH": "STRANGE_STAINS_IN_MOUTH",
    "ORAL_ULCERS": "STRANGE_STAINS_IN_MOUTH",
    "BLACK_STOOLS": "STOOL_PROBLEMS",
    "BLOOD_IN_THE_STOOL": "STOOL_PROBLEMS",
    "CONSTIPATION": "STOOL_PROBLEMS",
    "PERSISTENT_DIARRHEA": "STOOL_PROBLEMS",
    "DIARRHEA": "STOOL_PROBLEMS",
    "EXCESSIVE_BLOATING": "STOOL_PROBLEMS",
    "HEPATITIS": "HEPATITIS",
    "BREAST_TENDERNESS": "ENLARGED_AND_SORE_BREASTS",
    "NERVOUSNESS_TENSION": "CRYING_ANXIETY_WORRY",
    "TARCZYCA": "THYROID_PROBLEMS",  # str match
    "NIEDOCZYNNOSC": "THYROID_PROBLEMS",
    "HASHIMOTO": "THYROID_PROBLEMS",
    "NIEPLODNOSC": "INFERTILITY",
    "INSULINOOPORNOSC": "INSULIN_RESISTANCE",
    "ENDOMETRIOZA": "ENDOMETRIOSIS",
    "DEPRESJA": "DEPRESSION",
    "ASTMA": "LUNG_DISEASE",
    "PCOS": "PCOS",
    "POLICYSTYCZNYCH": "PCOS",
    "NOWOTWOR": "TUMOR",
    "KAMICA": "RECURRENT_KIDNEY_INFECTIONS",
    "NADCISNIENIE": "HIGH_BLOOD_PRESSURE",

    "ANAEMIA": "ANEMIA",
    "ADULT_T_CELL_LEUKEMIA": "TUMOR",
    "LEUKEMIA": "TUMOR",
    "BRAIN_TUMORS": "TUMOR",
    "BREAST_CANCER": "TUMOR",
    "CANCER_OF_THE_CERVIX_OVARIAN_OR_UTERUS": "TUMOR",
    "CESARKA": "CAESARIAN",
    "CESARSKIE": "CAESARIAN",
    "CHOLELITHIASIS": "CHOLECYSTOLITHIASIS",
    "CRYING_ANXIETY_WORRY": "MENTAL_ISSUES",
    "DEPRESSION": "MENTAL_ISSUES",
    "FEVER_ABOVE_40": "FEVER",
    "FEVER_HEADACHE_LAST_7_DAYS": "FEVER",
    "LONG_LASTING_FEVER": "FEVER",
    "HAEMOLYTIC_ANEMIA": "ANEMIA",
    "HEADACHES_OR_HEADACHES": "HEADACHES",
    "HEAVY_VOLUME_ACNE": "SEVERE_ACNE",
    "HEPATITIS_A": "HEPATITIS",
    "HEPATITIS_B": "HEPATITIS",
    "HEPATITIS_C": "HEPATITIS",
    "HEPATITIS_OF_ANOTHER_TYPE": "HEPATITIS",
    "INFECTION_WITH_URETHRITIS": "URETHRITIS",
    "INFECTION_WITH_URETHRITIS": "URETHRITIS",
    "RECURRENT_URETHRITIS": "URETHRITIS",
    "BLOOD_IN_THE_URINE": "URETHRITIS",
    "INFLAMMATORY_SKIN_DISEASES": "SEVERE_ACNE",
    "JAJOWODU": "OBSTRUCTION_FALLOPIAN_TUBE",
    "LAPAROSKOPIA": "SURGICAL_LAPAROSCOPY",
    "LAPAROTOMIA": "SURGICAL_LAPAROSCOPY",
    "LYZECZKOWANIE": "SPOONING",
    "METAL_ILLNESSES": "MENTAL_ILLNESSES",
    "MIESNIAKA": "MYOMIA",
    "MIGDALKOW": "TONSILS",
    "OVARIAN_CYSTS_REQUIRING_SURGERY": "OVARIAN_CYSTS",
    "POLIPA": "PYLYPS",
    "POLYCYSTIC_OVARIAN_DISEASE": "PCOS",
    "POZAMACICZNEJ": "ECTOPIC_PREGNANCY",
    "PRZEPUKLINA": "HERNIA",
    "RECURRENT_VAGINAL_INFECTIONS": "RECURRENT_VAGINITIS",
    "TORBIELI": "OVARIAN_CYSTS",
    "SHORTSIGHTEDNESS": "VISION_PROBLEMS",
    "VISION_PROBLEMS_BEFORE_60": "VISION_PROBLEMS",
    "WYROSTEK": "APPENDEKTOMIA",
    "ZOLCIOWEGO": "APPENDEKTOMIA",
    "HISTEROSURGICAL_LAPAROSCOPY": "SURGICAL_LAPAROSCOPY",
    "REMOVAL_PREGNANCY": "ECTOPIC_PREGNANCY",
    "EGG_RECEPTION": "EGG_SEMEN_RECEPTION",
    "EMBRYOS_RECEPTION": "EGG_SEMEN_RECEPTION",
    "SEMEN_RECEPTION": "EGG_SEMEN_RECEPTION",
    "ET": "ET_IVF",
    "ETM": "ET_IVF",
    "IVF": "ET_IVF",
    "IVF_ICSI": "ET_IVF",
    "SURGICAL_HYSTEROSCOPY": "SURGICAL_LAPAROSCOPY",
    "OBSTRUCTION_FALLOPIAN_TUBE": "DRAINAGE_FALLOPIAN_TUBES",

    "ADULT_ACNE": "SEVERE_ACNE",
    "ANOREXIA": "WEAK_APPETITE",
    "BOW": "MENTAL_ISSUES",
    "NEUROSIS": "MENTAL_ISSUES",
    "ELEVATED_LIVER_TEST": "LIVER_ENLARGEMENT",
    "FIBROIDS": "MYOMIA",
    "INFLAMMATORY_BOWEL_DISEASE": "INTERSTITIAL_BLEEDING",
    "INTESTINAL_BACTERIA": "INTERSTITIAL_BLEEDING",
    "PERSISTENT_PAIN_IN_THE_LUMBAR_AREA": "PERSISTENT_PAIN_IN_THE_NECK_AREA",
    "POLYP": "PYLYPS",

    "SEVERE_ACNE": "PERSISTENT_RASH",
    "VERY_DRY_SKIN": "PERSISTENT_RASH",
    "GASES_CRAMPS_PAIN": "STOOL_PROBLEMS",
    "DEGENERATIVE_DEGENERATION_OF_THE_SPINE": "PERSISTENT_PAIN_IN_THE_NECK_AREA",
    "PERSISTENT_COUGH": "LUNG_DISEASE",
    "SHORTNESS_OF_BREATH": "LUNG_DISEASE",
    "MINOR_PELVIC_INFLAMMATORY_DISEASE": "INFLAMMATION_IN_PELVIS",
    "HEADACHES_OR_MIGRAINE": "HEADACHES"






}


def process_duplicate_binary_numeric(
    data: pd.DataFrame,
    duplicates_numeric: Dict[str, List[int]] = duplicates_numeric,
    duplicates_binary: Dict[str, List[int]] = duplicates_binary,
) -> pd.DataFrame:
    """Przygotowanie pytań typu numeric, binary, single select, zduplikowanych w kolumnach: pytanie_pl oraz odpowiedzi.

    Strategia dla pytań typu numeric: ze zduplikowanych pytań funkcja zwraca wartość, gdzie ID pytania jest wyższe (pacjent poźniej odpowiedział na to samo pytanie)
    Strategia dla pytań typu binary: ze zduplikowanych pytań funkcja zwraca wartość maksymalną wzdłuż rekordów

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych (przeprocesowane pytania)
    duplicates_numeric : Dict[str, List[int, int]], optional
        Słownik z listą zduplikowanych pytań typu numeric, by default duplicates_numeric
    duplicates_binary : Dict[str, List[int, int]], optional
        Słownik z listą zduplikowanych pytań typu binary, by default duplicates_binary

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi, gdzie zdublowane pytania/odpowiedzi dla pytań binary oraz numeric są połączone.
    """
    for key, value in duplicates_numeric.items():

        value = [str(x) for x in value]
        sub_data_numeric = data[
            data.columns[np.isin([x[: x.find("_")]
                                 for x in data.columns], value)]
        ]

        sub_data_numeric.columns = sub_data_numeric.columns.str.extract(
            r"(\d*)_.*", expand=False
        )

        if sub_data_numeric.columns[1] > sub_data_numeric.columns[0]:
            sub_data_numeric[sub_data_numeric.columns[1]].fillna(
                sub_data_numeric[sub_data_numeric.columns[0]], inplace=True)
            sub_data_numeric.drop(
                sub_data_numeric.columns[0], axis=1, inplace=True)

        else:
            sub_data_numeric[sub_data_numeric.columns[0]].fillna(
                sub_data_numeric[sub_data_numeric.columns[1]], inplace=True)
            sub_data_numeric.drop(
                sub_data_numeric.columns[1], axis=1, inplace=True)

        sub_data_numeric.columns = [
            "_".join(value) + "_" + key.replace(" ", "_")]

        data = data.drop(
            columns=data.columns[
                np.isin([x[: x.find("_")] for x in data.columns], value)
            ],
            inplace=False,
        )

        data = data.merge(
            sub_data_numeric, how="left", left_index=True, right_index=True
        )

    for key, value in duplicates_binary.items():

        value = [str(x) for x in value]
        sub_data_binary = data[
            data.columns[np.isin([x[: x.find("_")]
                                 for x in data.columns], value)]
        ]

        sub_data_binary = sub_data_binary.max(axis=1, skipna=True)
        sub_data_binary = sub_data_binary.rename(
            "_".join(value) + "_" + key.replace(" ", "_")
        )

        data.drop(
            columns=data.columns[
                np.isin([x[: x.find("_")] for x in data.columns], value)
            ],
            inplace=True,
        )

        data = data.merge(
            sub_data_binary, how="left", left_index=True, right_index=True
        )

    return data


def process_duplicate_single_select(
    data: pd.DataFrame,
    duplicates_single_select: Dict[str, List[int]] = duplicates_single_select,
    mapping_for_duplicates: Dict[str, List[int]] = mapping_for_duplicates,
) -> pd.DataFrame:
    """Przygotowanie pytań typu single select, zduplikowanych w kolumnach: pytanie_pl oraz odpowiedzi.

    Strategia dla pytań single select: ze zduplikowanych pytań funkcja zwraca wartość maksymalną wzdłuż rekordów, pytania pogrupowane są według odpowiedzi - do tych pytań zastosowane jest również
    mapowanie ze słownika mapping_for_duplicates, które ujednolica odpowiedzi zduplikowanych pytań by umożliwić poźniejsze grupowanie według tych właśnie odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych (przeprocesowane pytania)

    duplicates_single_select : Dict[str, List[int, int]], optional
        Słownik z listą zduplikowanych pytań typu single select, by default duplicates_single_select

    mapping_for_duplicates : Dict[str, List[int, int]], optional
        Słownik z listą mapowania podobnych odpowiedzi

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi, gdzie zdublowane pytania/odpowiedzi są połączone.
    """

    for key, value in duplicates_single_select.items():

        value = [str(x) for x in value]
        sub_data = data[
            data.columns[np.isin([x[: x.find("_")]
                                 for x in data.columns], value)]
        ]
        value_join = "_".join(value)

        if value_join in mapping_for_duplicates.keys():
            for glupi, krystek in mapping_for_duplicates[value_join].items():
                sub_data.columns = sub_data.columns.str.replace(glupi, krystek)
        pattern = sub_data.columns.str.extract(r".*:(.*)", expand=False)
        sub_data = sub_data.groupby(pattern, axis=1).max()

        prefix = "_".join(value) + "_" + key.replace(" ", "_") + ":"
        sub_data = sub_data.add_prefix(prefix=prefix)

        data.drop(
            columns=data.columns[
                np.isin([x[: x.find("_")] for x in data.columns], value)
            ],
            inplace=True,
        )

        data = data.merge(sub_data, how="left",
                          left_index=True, right_index=True)

    return data


def process_calculate_bmi(
    data: pd.DataFrame, question_id_waga: int = 90, question_id_wzrost: int = 92
) -> pd.DataFrame:
    """process_calculate_bmi oblicza BMi na podstawie kolumn waga oraz wzrot (transformuje wzrost na m oraz usuwa z wejściowej tabeli kolumny waga oraz wzrost)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id_waga : int, optional
        id_question pytania o wagę pacjentki, by default 90
    question_id_wzrost : int, optional
        id_question pytania o wzrost pacjentki, by default 92
    """
    bmi = (data["90_Waga"] / (data["92_Wzrost"] / 100) ** 2).round(1).rename(
        str(question_id_waga) + "_" + str(question_id_wzrost) + "_" + "BMI"
    )
    data = data.merge(bmi, how="left", left_index=True, right_index=True)
    data.drop(["90_Waga", "92_Wzrost"], axis=1, inplace=True)

    return data


def process_multiselect_mapping(
    data: pd.DataFrame, multiselect_mapping: Dict[str, str] = multiselect_mapping, ids: List[int] = mapping_answers, fillna: bool = True
) -> pd.DataFrame:
    """Funkcja grupująca te same odpowiedzi na pytania z sekcji binary_multiselect_DETAILED_INTERVIEW oraz mapująca odpowiedzi do tych pytań.  

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych przeprocesowanych
    multiselect_mapping : List[int], optional
        Słownik z mapowaniem tych samych odpowiedzi, by default multiselect_mapping

    Returns
    -------
    pd.DataFrame
        Przeprocesowany zbiór danych z uwzględnieniem mapowania.
    """
    ids = [str(x) for x in ids]
    column_subset = data.columns[np.isin([x[: x.find("_")]
                                          for x in data.columns], ids)]

    sub_data = data[column_subset].copy()
    sub_data.columns = sub_data.columns.str.replace(
        '__LABEL', '')
    for key, value in multiselect_mapping.items():
        sub_data.columns = sub_data.columns.str.replace(
            ('\\b' + key + '\\b'), value, regex=True)
    pattern = sub_data.columns.str.extract(r".*:(.*)", expand=False)
    sub_data = sub_data.groupby(pattern, axis=1).max()
    prefix = (
        "Objaw_lub_procedura:"
    )
    sub_data = sub_data.add_prefix(prefix=prefix)
    if fillna:
        sub_data.fillna(0, inplace=True)
    sub_data = sub_data[sub_data.columns[sub_data.sum() > 30]]
    data.drop(
        columns=column_subset,
        inplace=True,
    )

    data = data.merge(sub_data, how="left",
                      left_index=True, right_index=True)

    data.columns = data.columns.str.replace("BINARY_", "")

    return data


def add_patient_id_column(patient_id_data: pd.DataFrame, processed_data: pd.DataFrame) -> pd.DataFrame:

    patient_id_data.drop_duplicates(inplace=True)

    processed_data.drop("wizyta_id", axis=1, inplace=True)

    result = processed_data.merge(
        patient_id_data[["wizyta_id", "patient_id"]], on="wizyta_id", how="left")
    # result.drop_duplicates("wizyta_id", inplace=True)
    result.set_index("wizyta_id", inplace=True)

    return result
# Ostateczne rozwiązanie


class p1_question_processing:
    """
    Klasa przygotowująca ankiety p1

    ...

    Attributes
    ----------
    data : pd.DataFrame
        Pełen zbiór danych ze źródła staging. Dane powinny zawierać odpowiedzi na ankiety P1 udzielone przez pacjentki. Dane można wgrać przy pomocy metody `load_data`.
    processed_data: pd.DataFrame
        Zawiera przeprocesowane wartości, gdzie 1 wiersz odpowiada 1 ankiecie - tabela jest wypełniana przez metodę `process_questions`.
    all_questions: List[int]
        Lista wszystkich id pytań z danych wejściowych
    str_match_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_str_match_questions.
    multiselect_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_multiselect_questions.
    numeric_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_numeric_questions.
    single_select_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_single_select_questions.
    binary_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_binary_questions.
    genetic_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_genetic_questions.
    map_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_map_questions.
    pregnancy_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_pregnancy_questions.
    binary_multiselect: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_binary_multiselect_questions.
    date_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyta jest funkcja process_date_questions.
    distinct_questions: List[int]
        Lista pytań, w których do przeprocesowania odpowiedzi użyte są dedykowane funcje na podstawie ich id.
    useless_questions: List[int]
        Lista pytań, które zostały uznane za nieistotne do modelowania.
    usefull_questions: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania.
    leki: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania: rekomendacja leki
    zalecenia_zlecenia: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania: zalecenia_zlecenia
    IDC10: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania: IDC10
    diagnoza_z_ankiety_kwalifikacyjnej: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania: diagnoza_z_ankiety_kwalifikacyjnej
    nastepny_krok_procesu: List[int]
        Lista pytań, które zostały uznane za istotne do modelowania: nastepny_krok_procesu

    Methods
    -------

    tutaj wpisz listę metod, np:
    load_data(self):
        load_data pobiera dane ze stagingu. Upewnij się, że w zmiennych środowiskowych są zdefiniowane wartości USER_staging, PASSWORD_staging, DOMAIN_staging.
    """

    def __init__(self, data=None):
        """__init__

        Parameters
        ----------
        data : pd.DataFrame
            Pełen zbiór danych ze źródła staging. Dane powinny zawierać odpowiedzi na ankiety P1 udzielone przez pacjentki, by default None
        """
        self.data: pd.DataFrame = data
        self.processed_data: pd.DataFrame = None
        self.patient_age_data: pd.DataFrame = None
        self.all_questions: List[int] = None
        self.str_match_questions: List[int] = None
        self.multiselect_questions: List[int] = None
        self.numeric_questions: List[int] = None
        self.single_select_questions: List[int] = None
        self.order_questions: List[int] = None
        self.binary_questions: List[int] = None
        self.genetic_questions: List[int] = None
        self.map_questions: List[int] = None
        self.pregnancy_questions: List[int] = None
        self.binary_multiselect: List[int] = None
        self.date_questions: List[int] = None
        self.distinct_questions: List[int] = None
        self.useless_questions: List[int] = None
        self.usefull_questions: List[int] = None
        self.leki: List[int] = None
        self.zalecenia_zlecenia: List[int] = None
        self.IDC10: List[int] = None
        self.diagnoza_z_ankiety_kwalifikacyjnej: List[int] = None
        self.nastepny_krok_procesu: List[int] = None

    def load_data(
        self,
        sql: str = "visit_p1_survey_staging",
        sql_age: str = "visit_p1_age",
        domain: str = "DOMAIN_staging",
    ):
        """load_data pobiera dane z data warehouse. Upewnij się, że w zmiennych środowiskowych są zdefiniowane wartości USER_data_warehouse, PASSWORD_data_warehouse, DOMAIN_data_warehouse."""
        print("Loading data...")
        self.data = get_query(
            sql,
            user="USER_staging",
            password="PASSWORD_staging",
            domain=domain,
        )
        self.data = self.data.drop_duplicates()
        self.patient_age_data = get_query(
            sql_age,
            user="USER_staging",
            password="PASSWORD_staging",
            domain=domain,
        )
        self.patient_age_data.drop_duplicates(inplace=True)
        print("Done.")

    def load_questions(self):
        """load_questions wprowadza wartości atrybutów klasy na podstawie predefiniowanych list odpowiadających klasyfikacji id pytań. Upewnij się, że wartości są w zmiennych globalnych."""
        unikalne_pytania_zbior_danych = self.data.id_question.unique()
        lists = [
            "str_match_questions_",
            "multiselect_questions_",
            "numeric_questions_",
            "single_select_questions_",
            "order_questions_",
            "binary_questions_",
            "genetic_questions_",
            "map_questions_",
            "pregnancy_questions_",
            "binary_multiselect_",
            "date_questions_",
            "distinct_questions_",
            "useless_questions_",
        ]
        sections = [
            "GENERAL_INTERVIEW_2",
            "GENERAL_INTERVIEW_3",
            "DETAILED_INTERVIEW",
            "GENETICS",
            "BASIC_DATA",
            "GENERAL_INTERVIEW_1",
            "PRE_INTERVIEW",
            "PHENOTYPIC_DATA",
            "FREESTYLE",
            "STATEMENTS",
        ]
        kombinacje = list(product(lists, sections))
        all_lists = [a + b for (a, b) in kombinacje]
        all_questions = []
        for lista in all_lists:
            items = globals()[lista]
            if len(items) > 0:
                all_questions.append(items)
        all_questions = list(chain.from_iterable(all_questions))
        if len(all_questions) != len(unikalne_pytania_zbior_danych):
            raise ValueError(
                f"Liczba pytań w listach wynosi {len(all_questions)}, zaś w zbiorze danych {len(unikalne_pytania_zbior_danych)}. Brakujące id: {list(set(unikalne_pytania_zbior_danych) - set(all_questions))}"
            )

        for lista in lists:
            r = re.compile(lista[:-1] + ".*")
            newlist = list(filter(r.match, list(globals().keys())))
            nested_list = itemgetter(*newlist)(globals())
            unnested_list = list(chain.from_iterable(nested_list))
            setattr(self, lista[:-1], unnested_list)

        self.all_questions = unikalne_pytania_zbior_danych
        self.usefull_questions = list(
            set(self.all_questions) - set(self.useless_questions)
        )

    def prepare_question(
        self,
        question_id: int,
        fillna:bool = True
    ) -> Union[pd.DataFrame, pd.Series]:
        """prepare_question procesuje wybrane pytanie ze zbioru danych

        Parameters
        ----------
        question_id : int
            id pytania

        Returns
        -------
        Union[pd.DataFrame, pd.Series]
            Tabela z przeprocesowanymi wartościami.
        """
        if question_id in self.str_match_questions:
            response = process_str_match_questions(
                data=self.data, question_id=question_id, fillna = fillna
            )
        elif question_id in self.single_select_questions:
            response = process_single_select_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.order_questions:
            response = process_order_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.multiselect_questions:
            response = process_multiselect_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.numeric_questions:
            response = process_numeric_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.binary_questions:
            response = process_binary_questions(
                data=self.data, question_id=question_id)
        elif question_id in self.genetic_questions:
            response = process_genetic_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.map_questions:
            response = process_map_questions(
                data=self.data, question_id=question_id)
        elif question_id in self.pregnancy_questions:
            response = process_pregnancy_questions(
                data=self.data, question_id=question_id, fillna=fillna
            )
        elif question_id in self.binary_multiselect:
            response = process_binary_multiselect_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.date_questions:
            response = process_date_questions(
                data=self.data, question_id=question_id
            )
        elif question_id in self.distinct_questions:
            dedicated_function = globals(
            )["process_question_" + str(question_id)]
            response = dedicated_function(
                data=self.data, question_id=question_id)
        return response

    def process_duplicate_question(self) -> pd.DataFrame:
        """process_duplicate_question łączy odpowiedzi na pytania o tej samej wartości znaczeniowej

        Returns
        -------
        pd.DataFrame
            self.processed_data po połączeniu duplikowanych pytań
        """
        print(f"Processing duplicate binary and numeric questions...")
        self.processed_data = process_duplicate_binary_numeric(
            self.processed_data)
        print(f"Done")
        print(f"Processing duplicate single select questions...")
        self.processed_data = process_duplicate_single_select(
            self.processed_data)
        self.processed_data['427_W_jakim_wieku_miała_Pani_pierwszą_miesiączkę?'].fillna(
            pd.cut(self.processed_data[ '112_W_jakim_wieku_miała_Pani_pierwszą_miesiączkę?'], bins = [0,10,13,15,100]).cat.codes.map({-1:np.nan, 0:0,1:1,2:2,3:3}),
            inplace = True
        )
        print(f"Done")

    def merge_questions(self) -> pd.DataFrame:
        """process_duplicate_question łączy odpowiedzi na pytania o tej samej wartości znaczeniowej

        Returns
        -------
        pd.DataFrame
            self.processed_data po połączeniu duplikowanych pytań
        """
        print(f"Merge duplicated questions...")
        self.processed_data['Objaw_lub_procedura:TUMOR'] = self.processed_data[[
            '557_559_561_Czy_choruje/chorowała_Pani_na_chorobę_nowotworową?', 'Objaw_lub_procedura:TUMOR']].max(axis=1)
        self.processed_data.drop(
            columns='557_559_561_Czy_choruje/chorowała_Pani_na_chorobę_nowotworową?', inplace=True)
        self.processed_data['Objaw_lub_procedura:MENTAL_ISSUES'] = self.processed_data[[
            '447_Czy_kiedykolwiek_przyjmowała_Pani_leki_przeciwdepresyjne_dłużej_niż_3_miesiące?', 'Objaw_lub_procedura:MENTAL_ISSUES']].max(axis=1)
        self.processed_data.drop(
            columns='447_Czy_kiedykolwiek_przyjmowała_Pani_leki_przeciwdepresyjne_dłużej_niż_3_miesiące?', inplace=True)
        self.processed_data['Maksymalna_amplituda_cykli'] = self.processed_data[
            "211_431_Najdłuższa_długość_cyklu_miesiączkowego_(liczba_dni_od_pierwszego_dnia_miesiączki_do_pierwszego_dnia_następnej_miesiączki)"
        ].sub(
            self.processed_data[
                "210_430_Najkrótsza_długość_cyklu_miesiączkowego_(liczba_dni_od_pierwszego_dnia_miesiączki_do_pierwszego_dnia_następnej_miesiączki)"
            ]
        )
        print(f"Done")

    def process_bmi(self) -> pd.DataFrame:
        """process_bmi wylicza BMI na podstawie kolumn wagi i wzrostu oraz usuwa kolumny ze zbioru danych

        Returns
        -------
        pd.DataFrame
            self.processed_data po obliczeniu BMI
        """
        print(f"Calculating BMI...")
        self.processed_data = process_calculate_bmi(self.processed_data)
        print(f"Done")

    def process_bmi(self) -> pd.DataFrame:
        """process_bmi wylicza BMI na podstawie kolumn wagi i wzrostu oraz usuwa kolumny ze zbioru danych

        Returns
        -------
        pd.DataFrame
            self.processed_data po obliczeniu BMI
        """
        print(f"Calculating BMI...")
        self.processed_data = process_calculate_bmi(self.processed_data)
        print(f"Done")

    def process_patient_id(self) -> pd.DataFrame:

        print("Adding patient_id column")
        self.processed_data = add_patient_id_column(
            self.data[["wizyta_id", "patient_id"]], self.processed_data)
        print("Done")

    def process_patient_age(self) -> pd.DataFrame:

        print("Adding patient_age column")
        self.processed_data = self.processed_data.merge(
            self.patient_age_data, left_index=True, right_on='wizyta_id', how='left')
        print("Done")

    def mapping_multiselect(self, fillna:bool = True) -> pd.DataFrame:
        """mapping_multiselect mapuje odpowiedzi z sekcji detiled_interview oraz grupuje je ze względu na ta samą odpowiedź

        Returns
        -------
        pd.DataFrame
            self.processed_data po połączeniu tych samych odpowiedzi
        """
        print(f"Processing mapping multiselect answers...")
        self.processed_data = process_multiselect_mapping(
            self.processed_data, fillna=fillna)
        print(f"Done")

    def process_questions(
        self, remove_duplicate_questions: bool = True, calculating_bmi: bool = True, mapping_multiselect_questions: bool = True, subset: str = None, patient_id_index: bool = False, fillna:bool = True
    ) -> pd.DataFrame:
        """process_questions przygotowuje gotową tabelę z przeprocesowanymi pytaniami, które są w liście usefull_questions

        Parameters
        ----------
        remove_duplicate_questions : bool
            Czy zduplikowane pytania powinny być zmergowane?, by default True

        mapping_multiselect_questions: bool
            Czy mapowac pytania z sekcji multiselect detailed_interview

        subset: str
        Do wyboru listy pytań z podziałem na cel modelowania: "leki", "zalecenia_zlecenia", "IDC10", "diagnoza_z_ankiety_kwalifikacyjnej", "nastepny_krok_procesu".

        Returns
        -------
        pd.DataFrame
            Gotowa tabela jest zapisywana w self.processed_data
        """
        self.processed_data = self.data[["wizyta_id"]].drop_duplicates()
        self.processed_data.set_index(
            self.processed_data.wizyta_id, inplace=True)
        self.leki = leki
        self.zalecenia_zlecenia = zalecenia_zlecenia
        self.IDC10 = IDC10
        self.diagnoza_z_ankiety_kwalifikacyjnej = diagnoza_z_ankiety_kwalifikacyjnej
        self.nastepny_krok_procesu = nastepny_krok_procesu

        if subset == 'leki':
            inter_set = self.leki
        elif subset == 'zalecenia_zlecenia':
            inter_set = self.zalecenia_zlecenia
        elif subset == 'IDC10':
            inter_set = self.IDC10
        elif subset == "diagnoza_z_ankiety_kwalifikacyjnej":
            inter_set = self.diagnoza_z_ankiety_kwalifikacyjnej
        elif subset == "nastepny_krok_procesu":
            inter_set = self.nastepny_krok_procesu
        else:
            inter_set = self.usefull_questions
            remove_duplicate_questions = True

        for question_id in set(self.usefull_questions).intersection(inter_set):
            print(f"Processing question {question_id}...")
            prepared_values = self.prepare_question(question_id, fillna=fillna)
            self.processed_data = pd.merge(
                self.processed_data,
                prepared_values,
                left_index=True,
                right_index=True,
                how="left",
            )
            print(
                f"Done. Current number of columns = {self.processed_data.shape[1]}")
            print(f"Current number of rows = {self.processed_data.shape[0]}")
        if calculating_bmi is True:
            self.process_bmi()
            print(
                f"Done. Number of columns after mapping= {self.processed_data.shape[1]}")
        if mapping_multiselect_questions is True:
            self.mapping_multiselect(fillna = fillna)
            print(
                f"Done. Number of columns after mapping= {self.processed_data.shape[1]}")
        if remove_duplicate_questions is True:
            self.process_duplicate_question()
            self.merge_questions()
        print(f"Final number of columns = {self.processed_data.shape[1]}")
        if patient_id_index is True:
            self.process_patient_id()
            self.process_patient_age()
        print("Done.")
