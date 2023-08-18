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
single_select_questions_BASIC_DATA = []
binary_questions_BASIC_DATA = []
distinct_questions_BASIC_DATA = [
    63, #Czy posiada Pan dzieci biologiczne?

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
    1,	#Imię
    3,	#Nazwisko
    5,	#Imię w dniu urodzenia, jeżeli inne niż obecne
    7,	#Nazwisko w dniu urodzenia, jeżeli inne niż obecne
    11,	#Data urodzenia
    13,	#Miejsce urodzenia
    15,	#Obywatelstwo
    17,	#Rodzaj dokumentu tożsamości
    19,	#Seria i numer
    21,	#Kraj
    23,	#Miejscowość	
    25,	#Kod pocztowy
    27,	#Ulica
    29,	#Numer domu / numer lokalu
    31,	#Kraj
    33,	#Miejscowość
    35,	#Kod pocztowy
    37,	#Ulica
    39,	#Numer domu / numer lokalu
    41,	#Numer telefonu
    43,	#Alternatywny numer telefonu
    45,	#Adres e-mail
    47,	#Alternatywny adres e-mail	
    49,	#Imię
    51,	#Nazwisko
    53,	#Numer telefonu
    55,	#Imię
    57,	#Nazwisko
    59,	#Numer telefonu
    61,	#Stan cywilny
    65, #Czy jest Pan ubezpieczony w NFZ?
    67, #Czy posiada Pan ubezpieczenie medyczne w innej firmie?
    69, #Z jakiej diagnostyki lub leczenia chciałby Pan skorzystać?
    71,	#Wykształcenie
    73,	#Zawód wyuczony
    75,	#Zawód wykonywany
    230, #Z jakiej diagnostyki lub leczenia chciałby Pan skorzystać?
    9, #PESEL DO NAPISANIA FUNKCJA
    652, #PESEL DO NAPISANIA FUNKCJA
    241,	#Drugie imię	null
    245,	#Kraj urodzenia
    246,	#Miejscowość urodzenia
    249,	#Kraj wydania dowodu tożsamości
    251,	#Województwo
    255,	#Preferowana forma kontaktu
    257,	#Zawód wyuczony
    289,	#Adres korespondencyjny inny niż zamieszkania
    353,	#Wykształcenie
    355,	#Zawód wykonywany
    357,	#Rodzaj dokumentu tożsamości
    237,    #Preferowana forma kontaktu
    654,    #Obywatelstwo
]

# DETAILED_INTERVIEW

single_select_questions_DETAILED_INTERVIEW = []
order_questions_DETAILED_INTERVIEW = []
binary_questions_DETAILED_INTERVIEW = [
    156,	#Czy czuje się Pan ogólnie zdrowy?
]
distinct_questions_DETAILED_INTERVIEW = []
str_match_questions_DETAILED_INTERVIEW = [
    158,    #Czy aktualnie się Pan leczy?
]
multiselect_questions_DETAILED_INTERVIEW = []
numeric_questions_DETAILED_INTERVIEW = []
genetic_questions_DETAILED_INTERVIEW = []
map_questions_DETAILED_INTERVIEW = []
pregnancy_questions_DETAILED_INTERVIEW = []
binary_multiselect_DETAILED_INTERVIEW = [
    160,    #Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z ogólnym stanem zdrowia?
    162,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    164,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    166,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    168,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    170,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    172,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    174,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    176,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    178,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    180,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    182,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    184,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    186,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
]
date_questions_DETAILED_INTERVIEW = []
useless_questions_DETAILED_INTERVIEW = []

# GENERAL_INTERVIEW_1

single_select_questions_GENERAL_INTERVIEW_1 = []
order_questions_GENERAL_INTERVIEW_1 = []
binary_questions_GENERAL_INTERVIEW_1 = [
    113,	#Czy kiedykolwiek rozpoznano u Pana niepłodność?
    446,    #Czy kiedykolwiek rozpoznano u Pana zaburzenia płodności?
]
distinct_questions_GENERAL_INTERVIEW_1 = [
    224,	#Jakie stosuje Pan lub stosował środki antykoncepcyjne?
    118,    #Jakie stosuje Pan lub stosował środki antykoncepcyjne?
    458,	#Czy stosuje Pan lub stosował środki antykoncepcyjne?
]
str_match_questions_GENERAL_INTERVIEW_1 = []
multiselect_questions_GENERAL_INTERVIEW_1 = []
numeric_questions_GENERAL_INTERVIEW_1 = [
    111,    #W jakim wieku po raz pierwszy się Pan ogolił (w latach)?
]
genetic_questions_GENERAL_INTERVIEW_1 = []
map_questions_GENERAL_INTERVIEW_1 = []
pregnancy_questions_GENERAL_INTERVIEW_1 = [
    120,    #Ile razy Pana partnerki były w ciąży?
    221,    #Ile razy Pana partnerki były w ciąży?
    443,    #Ile razy Pana partnerki były w ciąży?
]
binary_multiselect_GENERAL_INTERVIEW_1 = []
date_questions_GENERAL_INTERVIEW_1 = [
    219,	#Czy kiedykolwiek miał Pan wykonywane badanie USG jąder lub gruczołu krokowego?
    115,    #Czy kiedykolwiek miał Pan wykonywane badanie USG jąder lub gruczołu krokowego?
    460,    #Czy kiedykolwiek miał Pan wykonywane badanie USG jąder lub gruczołu krokowego?

]
useless_questions_GENERAL_INTERVIEW_1 = []

# GENERAL_INTERVIEW_2

single_select_questions_GENERAL_INTERVIEW_2 = []
order_questions_GENERAL_INTERVIEW_2 = []
binary_questions_GENERAL_INTERVIEW_2 = []
distinct_questions_GENERAL_INTERVIEW_2 = []
str_match_questions_GENERAL_INTERVIEW_2 = [
    239,	# Czy jest Pan pod stałą opieką lekarza?
    130,    # Czy miał Pan w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie żylaków powrózka, itp.)?
    122,    # Czy przyjmuje Pan obecnie lub przyjmował jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)?
    128,    # Czy jest Pan lub był pod opieką lekarza z powodu leczenia jakichś chorób?
    463,    # Czy jest Pan lub był Pan pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    490,    # Czy przyjmuje Pan obecnie lub przyjmował Pan w ciągu ostatnich 12 miesięcy jakiekolwiek leki?
]
multiselect_questions_GENERAL_INTERVIEW_2 = [
    138, # Jeśli obecnie choruje Pan, kiedykolwiek chorował lub był Pan kiedykolwiek leczony z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    142, # Proszę zaznaczyć szczepienia, które Pan kiedykolwiek miał
    496, # Proszę zaznaczyć szczepienia, które Pan kiedykolwiek miał
]
numeric_questions_GENERAL_INTERVIEW_2 = []
genetic_questions_GENERAL_INTERVIEW_2 = []
map_questions_GENERAL_INTERVIEW_2 = []
pregnancy_questions_GENERAL_INTERVIEW_2 = []
binary_multiselect_GENERAL_INTERVIEW_2 = []
date_questions_GENERAL_INTERVIEW_2 = []
useless_questions_GENERAL_INTERVIEW_2 = [
    124,	#Czy jest Pan uczulony (na leki, pokarm, środki chemiczne, inne)?
    492,    #Czy jest Pan uczulony (na leki, pokarm, środki chemiczne, inne)?
    126,    #Czy miał Pan alergie w wieku dziecięcym (których obecnie Pan nie ma)?
    132,	#Czy kiedykolwiek był Pan w szpitalu z powodów medycznych innych niż zabiegi chirurgiczne?
    494,    #Czy kiedykolwiek był Pan w szpitalu z powodów medycznych innych niż zabiegi chirurgiczne?
    134,	#Czy miał Pan kiedykolwiek złamane kości?
    136,	#Ile dni w ciągu ostatnich 12 miesięcy  nie mógł Pan pracować  z powodu choroby (np. przeziębienia, grypy, zabiegów, wypadków)?
    140,	#Proszę dodać uwagi odnośnie zaznaczonych odpowiedzi
]

# GENERAL_INTERVIEW_3

single_select_questions_GENERAL_INTERVIEW_3 = []
order_questions_GENERAL_INTERVIEW_3 = []
binary_questions_GENERAL_INTERVIEW_3 = [
    144,	#Czy pije Pan alkohol?
    643,    #Czy pije Pan alkohol?
    147,	#Czy kiedykolwiek stosował Pan narkotyki?
    467,    #Czy kiedykolwiek stosował Pan narkotyki?
    146,    #Czy kiedykolwiek stosował Pan dożylne, domięśniowe lub podskórne iniekcje leków z powodów niemedycznych?
    466,    #Czy kiedykolwiek stosował Pan dożylne, domięśniowe lub podskórne iniekcje leków z powodów niemedycznych?
]
distinct_questions_GENERAL_INTERVIEW_3 = [
    145,	#Czy pali/palił Pan papierosy?
    465,    #Czy pali/palił Pan papierosy?
]
str_match_questions_GENERAL_INTERVIEW_3 = []
multiselect_questions_GENERAL_INTERVIEW_3 = []
numeric_questions_GENERAL_INTERVIEW_3 = []
genetic_questions_GENERAL_INTERVIEW_3 = []
map_questions_GENERAL_INTERVIEW_3 = []
pregnancy_questions_GENERAL_INTERVIEW_3 = []
binary_multiselect_GENERAL_INTERVIEW_3 = []
date_questions_GENERAL_INTERVIEW_3 = []
useless_questions_GENERAL_INTERVIEW_3 = [
    148,	#Proszę zaznaczyć wszystkie środki, które Pan kiedykolwiek stosował
    502,    #Proszę zaznaczyć wszystkie środki, które Pan kiedykolwiek stosował
    149,	#Czy kiedykolwiek narażony był Pan na nadmierne ilości szkodliwych czynników fizycznych, chemicznych lub biologicznych?
    504,	#Czy kiedykolwiek narażony był Pan na nadmierne ilości szkodliwych czynników fizycznych, chemicznych lub biologicznych?	
]

# GENETICS

single_select_questions_GENETICS = []
order_questions_GENETICS = []
binary_questions_GENETICS = [
    188,	#Czy był Pan adoptowany?
    189,	#Czy urodził się Pan z jakąkolwiek wadą wrodzoną (wady serca, rozszczep wargi lub podniebienia itp.)?
    190,	#Czy występują w Pana rodzinie jakieś znane Panu choroby genetyczne lub wady wrodzone?
    191,    #Czy urodziło się w Pana rodzinie dziecko z chorobą genetyczną lub wadą wrodzoną?
    506,	#Czy w Pana rodzinie występowały niepowodzenia rozrodu?
    508,	#Czy w Pana rodzinie występowały poronienia samoistne (>2 u jednej kobiety)?
    510,    #Czy w Pana rodzinie występowały urodzenia martwego płodu?
    512,    #Czy w Pana rodzinie występowała niepłodność?
    514,	#Czy w Pana rodzinie występowały poważne wady genetyczne?
    516,	#Czy w Pana rodzinie występowały wady wrodzone wykrywane w trakcie ciąży u płodu?
    520,    #Czy w Pana rodzinie występowały choroby genetycznie uwarunkowane lub choroby, które miały niewyjaśnione podłoże?
    522,	#Czy w Pana rodzinie są różne osoby, które chorowały na choroby nowotworowe w podobny sposób?
    524,	#Czy w Pana rodzinie są osoby z niepełnosprawnością umysłową/upośledzeniem umysłowym o niewyjaśnionej przyczynie?
    526,	#Czy u kogoś w Pana rodzinie wystąpiły nieprawidłowości rozwoju płciowego?
    528,	#Czy w Pana rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?
    530,	#Czy w Pana rodzinie wystąpiły przypadki translokacyjnego zespołu Downa?
    532,	#Czy w Pana rodzinie wykryto przypadki aberracji chromosomowej lub nietypowego wariantu chromosomowego w badaniu prenatalnym u płodu?
    534,	#Czy w Pana rodzinie wystąpiły przypadki nietypowego wariantu chromosomowego?	
    536,	#Czy w Pana rodzinie wystąpiły przypadki pierwotnego lub wtórnego braku miesiączki?
    538,	#Czy w Pana rodzinie wystąpiły przypadki azoospermii lub oligozoospermia w badaniu nasienia?
    540,	#Czy w Pana rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?
    546,	#Czy u Pana lub w Pana rodzinie wystąpiły przypadki zespołu niestabilności chromosomów (Zespół Blooma, Anemia Fanconiego, Ataxia Teleangiectasia, Zespół Nijmegen)?
    548,	#Czy w Pana rodzinie wystąpiły przypadki mutacji punktowych, delecji, duplikacji w obrębie genu/ów?
    550,	#Czy w Pana rodzinie wystąpiły przypadki wykrycia mutacji punktowych, delecji, duplikacji w obrębie genu/genów w badaniu prenatalnym u płodu pacjentki?
    552,	#Czy w Pana rodzinie wystąpiły przypadki chorób mitochondrialnych?
    554,	#Czy w Pana rodzinie wykryto u płodu w badaniu prenatalnym choroby mitochondrialne?
    556,	#Czy choruje Pan na chorobę nowotworową?
    558,	#Czy chorował Pan na chorobę nowotworową?	
    560,    #Czy chorował Pan na chorobę nowotworową przed 40 r.ż. lub zachorowanie wystąpiło w narządach parzystych obustronnie (obie piersi zajęte) lub jednocześnie w więcej niż jednym narządzie?
    566,	#Czy stwierdzono u Pana: wrodzony hipogonadyzm, zaburzenia rozwoju narządów płciowych, nieprawidłowy rozwój trzeciorzędowych cech płciowych (takich jak niedorozwój piersi, nieprawidłowa budowa ciała i nieprawidłowe – nadmierne owłosienie)?
    596,	#Czy był/jest Pan narażony na działanie promieniowania jonizującego, promieni rentgena, radioterapia?

]
distinct_questions_GENETICS = []
str_match_questions_GENETICS = []
multiselect_questions_GENETICS = []
numeric_questions_GENETICS = []
genetic_questions_GENETICS = [
    403,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    405,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    407,    #Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    574,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    576,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    578,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    580,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    586,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    588,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    592,	#Czy kiedykolwiek zaobserwował Pan inne niepokojące objawy?
    600,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?	
    602,	#Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    604,	#Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
]
map_questions_GENETICS = []
pregnancy_questions_GENETICS = []
binary_multiselect_GENETICS = []
date_questions_GENETICS = []
useless_questions_GENETICS = [
    192,	#Czy jest Pan rasy kaukaskiej?
    194,	#Czy jest Pan rasy negroidalnej?
    193,    #Czy jest Pan pochodzenia żydowskiego?
    195,	#Czy jest Pan pochodzenia śródziemnomorskiego (greckiego, włoskiego) lub azjatyckiego (chińskiego, południowo-wschodnio-azjatyckiego)?
    234,	#Czy jest Pan pochodzenia żydowskiego?
    518,    #Czy w Pana rodzinie występowały małżeństwa między krewnymi?
    542,    #Czy u kogoś w Pana rodzinie wystąpiły zaburzenia wzrostu (np. niski wzrost) lub nieproporcjonalny wzrost?
    544,	#Czy w Pana rodzinie wystąpiły przypadki znacznego zaburzenia wzrostu, wysoki wzrost u mężczyzny?
    562,	#Czy jest Pan spokrewniony ze współmałżonką/partnerką?
    564,	#Czy występują u Pana wrodzone zmiany morfologiczne budowy ciała?
    595,	#Czy był/jest Pan narażony na działanie związków mutagennych?
    597,	#Czy w Pana rodzinie wystąpiły przypadki znacznego zaburzenia wzrostu, niski wzrost u kobiety?
]

# PRE_INTERVIEW

single_select_questions_PRE_INTERVIEW = [
    # 101,	#Jak długo stara się Pan zostać ojcem?
    # 103,	#Jak często odbywa Pan stosunki płciowe?
]
order_questions_PRE_INTERVIEW = [
    101,	#Jak długo stara się Pan zostać ojcem?
    103,	#Jak często odbywa Pan stosunki płciowe?
]
binary_questions_PRE_INTERVIEW = []
distinct_questions_PRE_INTERVIEW = [
    107,    #Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miał Pan wykonywane jakiekolwiek procedury medyczne (np. zapłodnienie pozaustrojowe, usunięcie żylaków powrózka)?
    109,    #Czy przyjmował Pan lub przyjmuje jakiekolwiek leki?
    225,    #Czy przyjmował Pan lub przyjmuje jakiekolwiek leki?
]
str_match_questions_PRE_INTERVIEW = []
multiselect_questions_PRE_INTERVIEW = []
numeric_questions_PRE_INTERVIEW = []
genetic_questions_PRE_INTERVIEW = []
map_questions_PRE_INTERVIEW = []
pregnancy_questions_PRE_INTERVIEW = []
binary_multiselect_PRE_INTERVIEW = []
date_questions_PRE_INTERVIEW = []
useless_questions_PRE_INTERVIEW = [
    99,	    #Czy leczył się Pan z powodu określonego wyżej?
    233,	#Przyczyna zgłoszenia się do Kliniki
    97,     #Przyczyna zgłoszenia się do Kliniki
    105,    #Czy w trakcie dotychczasowej diagnostyki miał Pan wykonywane jakiekolwiek badania?
    218,    #Czy w trakcie dotychczasowej diagnostyki miał Pan wykonywane jakiekolwiek badania?
    235,	#Czy w trakcie dotychczasowej diagnostyki miał Pan wykonywane jakiekolwiek badania?
]

# PHENOTYPIC_DATA

single_select_questions_PHENOTYPIC_DATA = [
    85, # Grupa krwi
    616, # Grupa krwi
    87, # Czynnik Rh
    618, # Czynnik Rh
]
order_questions_PHENOTYPIC_DATA = []
binary_questions_PHENOTYPIC_DATA = []
distinct_questions_PHENOTYPIC_DATA = []
str_match_questions_PHENOTYPIC_DATA = []
multiselect_questions_PHENOTYPIC_DATA = []
numeric_questions_PHENOTYPIC_DATA = [
    89, # Waga (treshold 45-160)
    91, # Wsrost (treshold 155-210)
]
genetic_questions_PHENOTYPIC_DATA = []
map_questions_PHENOTYPIC_DATA = []
pregnancy_questions_PHENOTYPIC_DATA = []
binary_multiselect_PHENOTYPIC_DATA = []
date_questions_PHENOTYPIC_DATA = []
useless_questions_PHENOTYPIC_DATA = [
    77,	#Rasa
    79,	#Kolor oczu
    81,	#Naturalny kolor włosów
    83,	#Struktura włosów
    93,	#Budowa ciała
    95,	#Etniczność
    291, #Rasa
    295, #Budowa ciała
    606, #Kolor skóry
    608, #Etnicność
    610, #Kolor oczu
    612, #Naturalny kolor włosów
    614, #Skręt włosów
]

# STATEMENTS

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
useless_questions_STATEMENTS = [204, 639]

# configs

drop_genetic_cols = [
    "405_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_oddechowym_(np._zapalenia_oskrzeli,_niewydolność_płuc_itp.)?:PNEUMONIA",
    "576_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_endokrynnym/metabolicznym_(np._powiększenie_tarczycy,_nietolerancja_ciepła_itp.)?:GOITRE",
    "576_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_układem_endokrynnym/metabolicznym_(np._powiększenie_tarczycy,_nietolerancja_ciepła_itp.)?:SHORT_STATURE",
    "580_Czy_kiedykolwiek_zaobserwował_Pan_niepokojące_objawy_związane_z_układem_płciowym_(np._brak_miesiączki,_trudności_w_zapłodnieniu_itp.)?:CRYPTORCHIDISM",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:ASTIGMATISM",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:FARSIGHTEDNESS",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:GLASSES_OR_CONTACT_LENSES_BEFORE_45",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:DEVIATED_NASAL_SEPTUM",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:NO_SMELL",
    "586_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_z_systemem_oczy_-_uszy_-_nos_(np._katar,_zapalenia_zatok,_zaburzenia_widzenia_itp.)?:DALTONISM",
    "588_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_ze_skórą_lub_włosami_(np._bardzo_sucha_skóra,_nadmiernie_wypadające_włosy_itp.)?:MULTIPLE_MOLES",
    "588_Czy_kiedykolwiek_zaobserwował_Pan_u_siebie_lub_najbliższej_rodziny_niepokojące_objawy_związane_ze_skórą_lub_włosami_(np._bardzo_sucha_skóra,_nadmiernie_wypadające_włosy_itp.)?:LARGE_DEEP_VEIN_VARICOSE",
    "593_Czy_kiedykolwiek_zaobserwował_Pan_inne_niepokojące_objawy?:ABNORMAL_BITE",
    "592_Czy_kiedykolwiek_zaobserwował_Pan_inne_niepokojące_objawy?:EARLY_DEATH",
    "592_Czy_kiedykolwiek_zaobserwował_Pan_inne_niepokojące_objawy?:OTHER_SITUATION",
]

mapping_answers = [
    160,  # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z ogólnym stanem zdrowia?
    162,   # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    164,  # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z systemem gardło - jama ustna - zęby?
    166, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    168,    # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    170, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    172, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem moczowo - płciowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    174, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    176, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    178, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z gruczołami piersiowymi (np. guzki piersi, tkliwość piersi itp.)?
    180, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z  układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    182, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    184, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    186, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    239, # Czy jest Pan pod stałą opieką lekarza?
    158, # Czy aktualnie się Pan leczy?
    130, # Czy miał Pan w przeszłości jakieś zabiegi operacyjne (usunięcie wyrostka robaczkowego, usunięcie żylaków powrózka, itp.)?
    138, # Jeśli obecnie choruje Pan, kiedykolwiek chorował lub był Pan kiedykolwiek leczony z powodów wymienionych poniżej, proszę zaznaczyć właściwe
    128, # Czy jest Pan lub był pod opieką lekarza z powodu leczenia jakichś chorób?
    463, # Czy jest Pan lub był Pan pod stałą opieką lekarza z powodu leczenia chorób przewlekłych?
    580, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem płciowym (np. brak miesiączki, trudności w zapłodnieniu itp.)?
    403, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem limfatycznym (np. anemia, powiększenie węzłów chłonnych itp.)?
    405, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem oddechowym (np. zapalenia oskrzeli, niewydolność płuc itp.)?
    407, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem pokarmowym (np. kamica pęcherzyka żółciowego, uporczywa biegunka itp.)?
    574, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem krążenia (np. omdlenia, duszności, słabe krążenie itp.)?
    576, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z układem endokrynnym/metabolicznym (np. powiększenie tarczycy, nietolerancja ciepła itp.)?
    578, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem moczowym (np. kamica nerkowa, nietrzymanie moczu itp.)?
    586, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z systemem oczy - uszy - nos (np. katar, zapalenia zatok, zaburzenia widzenia itp.)?
    588, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane ze skórą lub włosami (np. bardzo sucha skóra, nadmiernie wypadające włosy itp.)?
    592, # Czy kiedykolwiek zaobserwował Pan inne niepokojące objawy?
    600, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane z  układem nerwowym (np. padaczka, zawroty głowy, omdlenia itp.)?
    602, # Czy kiedykolwiek zaobserwował Pan niepokojące objawy związane z układem kostno - mięśniowo - stawowym (np. obrzęki stawów, choroby reumatyczne itp.)?
    604, # Czy kiedykolwiek zaobserwował Pan u siebie lub najbliższej rodziny niepokojące objawy związane ze stanem psychicznym (np. depresja, nerwica, stany lękowe itp.)?
    107, # Czy w trakcie dotychczasowej diagnostyki lub/i leczenia miał Pan wykonywane jakiekolwiek procedury medyczne (np. zapłodnienie pozaustrojowe, usunięcie żylaków powrózka)?

]

# tresholds

threshold = {
    63: {"min":0, "max": 7},
    89: {"min":45, "max": 160},
    91: {"min":155, "max":210},
    111: {"min": 5, "max" : 100}}

mapping_order_scale = {
    "SINCE_FEW_MONTHS": 0,
    "SINCE_FEW_YEARS": 1,
    "MULTIPLE_YEARS": 2,
    "SEVERAL_TIMES_DAY": 3,
    "ONE_OR_SEVERAL_TIMES_DAY": 3,
    "FEW_TIMES_WEEK": 2,
    "OCCASIONALLY": 0,
    "SEVERAL_TIMES_MONTH": 1,
}

str_match_questions_settings = {
    239: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "DEPRESJA",
            "ASTMA",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
            "CUKRZYCA",
            "ASTMA",
            "SPERMIA"
        ],
    },
    130: {
        "pattern": '\["([^"]*)"',
        "list_of_classes": [
            "LAPAROSKOPIA",
            "HISTEROSKOPIA",
            "WYROSTEK",
            "LAPAROTOMIA",
            "MIGDALKOW",
            "ZOLCIOWEGO",
            "TORBIELI",
            "PRZEPUKLINA",
            "POLIPA",
            "MIESNIAKA",
            "HISTEROLAPAROSKOPIA",
            "APPENDEKTOMIA",
            "HSG",
            # "CHOLECYSTEKTOMIA",
            "ABLACJA",
        ],
    },
    158: {
        "pattern": None,
        "list_of_classes": [
            "TARCZYCA",
            "NIEDOCZYNNOSC",
            "HASHIMOTO",
            "NIEPLODNOSC",
            "INSULINOOPORNOSC",
            "DEPRESJA",
            "ASTMA",
            "NOWOTWOR",
            "KAMICA",
            "NADCISNIENIE",
            "CUKRZYCA",
            "ASTMA",
            "SPERMIA"
        ],
    },
    122: {
        "pattern": '{"user": \[\["([^"]*)"',
        "list_of_classes": [
                "CYNK",
                "ACARD",
                "WIT_C",
                "ACESAN",
                "GLUCOPHAGE",
                "FERTILMAN",
                "ANDROSTATIN",
                "JOVESTO",
                "PHOSTAL",
                "VALDOXAN",
                "PROFERTIL",
                "PARENTON",
                "PROXEED",
                "POLPRIL",
                "NUCLEOX",
                "FOLIOWY",
                "CLOSTILBEGYT",
                "MICARDIS",
                "LECALPIN",
                "TRITACE",
                "ANDROVIT",
                "TADALAFIL",
                "TRIPLIXAM",
                "EUTHYROX",
                "LETROX",
                "D"
                "D3",
                "BROMERGON",
                "MAGNEZ"
        ],
    },
    128: {
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
    463: {
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
            "WZW"
        ],
    },
    490: {
        "pattern": '{"user": \[\["([^"]*)"',
        "list_of_classes": [
                "CYNK",
                "ACARD",
                "WIT_C",
                "ACESAN",
                "GLUCOPHAGE",
                "FERTILMAN",
                "ANDROSTATIN",
                "JOVESTO",
                "PHOSTAL",
                "VALDOXAN",
                "PROFERTIL",
                "PARENTON",
                "PROXEED",
                "POLPRIL",
                "NUCLEOX",
                "FOLIOWY",
                "CLOSTILBEGYT",
                "MICARDIS",
                "LECALPIN",
                "TRITACE",
                "ANDROVIT",
                "TADALAFIL",
                "TRIPLIXAM",
                "EUTHYROX",
                "LETROX",
                "D"
                "D3",
                "BROMERGON",
                "MAGNEZ"
        ],
    },
}

# functions

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

    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response

# process genetic questions

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

# process_str_match_questions
def process_str_match_questions(
    data: pd.DataFrame,
    question_id: int,
    str_match_questions_settings: Dict[
        float, Dict[str, Any]
    ] = str_match_questions_settings,
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
    final_df.fillna(0, inplace=True)
    final_df = match_to_columns(final_df, response)
    final_df = final_df.add_prefix(prefix=prefix)
    final_df.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return final_df

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

# process_pregnancy_questions
def process_pregnancy_questions(data: pd.DataFrame, question_id: int) -> pd.DataFrame:
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
    response.fillna(0, inplace=True)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response

# process_distinct_questions

def process_question_63(data: pd.DataFrame, question_id: int = 63) -> pd.DataFrame:
    """
    Przygotowanie pytania o liczbę biologicznych bombelków.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 63

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

def process_question_109(data: pd.DataFrame, question_id: int = 109) -> pd.DataFrame:
    """Przygotowanie pytania: czy przyjmowała/przyjmuje Pani jakiekolwiek leki (przyjmowane leki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 109

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

def process_question_118(data: pd.DataFrame, question_id: int = 118) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 118

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

def process_question_107(data: pd.DataFrame, question_id: int = 107) -> pd.DataFrame:
    """Przygotowanie pytania: czy w trakcie dotychczasowej diagnostyki miała Pani przeprowadzane
    jakiekolwiek procedury medyczne np. udrożnienie jajników? (rodzaj diagnostyki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 107

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

    cols_to_explode = [
        "ET",
        "ETM",
        "IUI",
        "IVF",
        "PGD",
        "TESE",
        "IVF_ICSI",
        "SEMEN_DONATION",
        "EMBRYO_BUSINESS",
        "EGG_BUSINESS_PARTNER",
        "REMOVAL_VARICOSE_VEINS",
        "REMOVAL_PREGNANCY_PARTNER",
    ]

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

def process_question_145(data: pd.DataFrame, question_id: int = 145) -> pd.DataFrame:
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

def process_question_224(data: pd.DataFrame, question_id: int = 224) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 224

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

def process_question_225(data: pd.DataFrame, question_id: int = 225) -> pd.DataFrame:
    """Przygotowanie pytania: czy przyjmowała/przyjmuje Pani jakiekolwiek leki (przyjmowane leki - 0/1).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 225

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

def process_question_465(data: pd.DataFrame, question_id: int = 465) -> pd.DataFrame:
    """
    Przygotowanie pytania o palenie papierosów.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 465

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

def process_question_458(data: pd.DataFrame, question_id: int = 458) -> pd.DataFrame:
    """
    Przygotowanie pytania o środki antykoncepcyjne (jedno z 2).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 458

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
    101,  # czas starania się o dziecko
    103,  # częstotliwość stosunków
]

# Mapowanie pytan

duplicates_numeric = {}
duplicates_binary = {
    "Czy kiedykolwiek stosował Pan dożylne, domięśniowe lub podskórne iniekcje leków z powodów niemedycznych?" : [146, 466],
    "Czy kiedykolwiek rozpoznano u Pana niepłodność?": [113, 446],
    "Czy kiedykolwiek stosował Pan narkotyki?": [147, 467],
    "Czy pije Pan alkohol?": [144, 643],
    "Czy występują w Pana rodzinie jakieś znane Panu choroby genetyczne lub wady wrodzone?": [
        190,
        191,
        514,
        520,
    ],
    "Czy choruje/chorował Pan na chorobę nowotworową?": [556, 558, 560],
    "Czy w Pana rodzinie wystąpiły przypadki aberracji chromosomowej (translokacji zrównoważonej, delecji)?": [
        528,
        532,
        534,
        546,
    ],
    "Czy w Pana rodzinie wystąpiły przypadki nieprawidłowej budowy narządów płciowych/obojnactwo?": [
        526,
        540,
        566,
    ],
    "Czy był/jest Pan narażony na działanie związków mutagennych?": [595, 596],
    "Czy był Pan adoptowany?": [188, 475]
}
duplicates_single_select = {
    "Czy_aktualnie_się_Pan_leczy?": [158, 239, 463, 128],
    "Grupa krwi": [85, 616],
    "Czynnik Rh": [87, 618],
    "Czy_stosuje_Pan_środki_antykoncepcyjne?": [118, 224, 458],
    "Ile razy Pana partnerki były w ciąży?" : [120, 221, 443],
    "Czy przyjmuje Pan obecnie lub przyjmowała jakiekolwiek leki (inne niż wymienione w zakładce Wywiad wstępny)": [
        109,
        122,
        225,
        490
    ],
    "Czy pali/palił Pan papierosy?": [145, 465],
    "Proszę zaznaczyć szczepienia, które Pan kiedykolwiek miał": [142, 496],
    #"Jak_często_odbywa_Pani_stosunki_płciowe?": [103]
}

mapping_for_duplicates = {

    "85_616": {
        "DONT_KNOW_BLOOD_GROUP":"DONT_KNOW_BLOOD_TYPE"
    },
    "103": {
        "ONE_OR_SEVERAL_TIMES_DAY": "SEVERAL_TIMES_DAY"
    },
    "158_239": {
        "TARCZYCA": "NIEDOCZYNNOSC",
    },
    "118_224_458": {
        "ABSTINENCE": "NO",
        "NONE": "NO",
        "FOAM_GEL": "SPERMICIDE",
        "INTERMITTENT": "NO",
        "LIGATION_OF_FALLOPIAN_TUBES": "PERMANENT_CONTRACEPTION",
        "LIGATON_OF_EP": "PERMANENT_CONTRACEPTION",
        "CALENDAR": "NO",
        "DIAPHRAGM": "CONDOM",
    },
    "109_122_225_490": {
        "ACARD": "ACETYLOSALICYLIC_ACID",
        "ACESAN": "ACETYLOSALICYLIC_ACID",
        "BROMERGON": "BROMOCRIPTINE",
        "TADALAFIL": "CLOSTILBEGYT",
        "D3": "D",
        "EUTHYROX": "LEVOTHYROXINE",
        "FOLIOWY": "FOLIC_ACID",
        "GLUCOPHAGE": "METFORMIN",
        "LETROX": "LEVOTHYROXINE",
        "MAGNEZ": "MAGNESIUM",
        "PROFERTIL": "MALE_SUPPLEMENT",
        "FERTILMAN": "MALE_SUPPLEMENT",
        "PARENTON": "MALE_SUPPLEMENT",
        "PROXEED": "MALE_SUPPLEMENT",
        "NUCLEOX" : "MALE_SUPPLEMENT",
        "ANDROVIT" : "MALE_SUPPLEMENT",
        "POLPRIL" : "ANGIOTENSIN",
        "MICARDIS" : "ANGIOTENSIN",
        "LECALPIN": "ANGIOTENSIN",
        "TRITACE": "ANGIOTENSIN",
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
    "EXCESSIVE_BLOATING": "GASES_CRAMPS_PAIN",
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
    "MINOR_PELVIC_INFLAMMATORY_DISEASE": "INFLAMMATION_IN_PELVIS"

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
#BMI

def process_calculate_bmi(
    data: pd.DataFrame, question_id_waga: int = 89, question_id_wzrost: int = 91
) -> pd.DataFrame:
    """process_calculate_bmi oblicza BMi na podstawie kolumn waga oraz wzrot (transformuje wzrost na m oraz usuwa z wejściowej tabeli kolumny waga oraz wzrost)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id_waga : int, optional
        id_question pytania o wagę pacjentki, by default 89
    question_id_wzrost : int, optional
        id_question pytania o wzrost pacjentki, by default 91
    """
    bmi = (data["89_Waga"] / (data["91_Wzrost"] / 100) ** 2).round(1).rename(
        str(question_id_waga) + "_" + str(question_id_wzrost) + "_" + "BMI"
    )
    data = data.merge(bmi, how="left", left_index=True, right_index=True)
    data.drop(["89_Waga", "91_Wzrost"], axis=1, inplace=True)

    return data

def process_multiselect_mapping(
    data: pd.DataFrame, multiselect_mapping: Dict[str, str] = multiselect_mapping, ids: List[int] = mapping_answers
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
    result.drop_duplicates("wizyta_id", inplace=True)
    result.set_index("wizyta_id", inplace=True)

    return result


# Ostateczne rozwiązanie


class p1_male_question_processing:
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
        self.patient_id_data: pd.DataFrame = None
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

    def load_data(
        self,
        sql: str = "visit_p1_survey_staging",
        sql_age: str = "visit_p1_male_age",
        domain: str = "DOMAIN_staging",
    ):
        """load_data pobiera dane z data warehouse. Upewnij się, że w zmiennych środowiskowych są zdefiniowane wartości USER_data_warehouse, PASSWORD_data_warehouse, DOMAIN_data_warehouse."""
        print("Loading data...")
        self.data = get_query(
            sql,
            user="USER_data_warehouse",
            password="PASSWORD_data_warehouse",
            domain=domain,
        )
        self.data = self.data.drop_duplicates()
        self.patient_id_data = self.data.copy()
        print("Done.")

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
                f"Liczba pytań w listach wynosi {len(all_questions)}, zaś w zbiorze danych {len(unikalne_pytania_zbior_danych)}. Brakujące id: {list(set(unikalne_pytania_zbior_danych) - set(all_questions))} lub {list(set(all_questions) - set(unikalne_pytania_zbior_danych))}"
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
                data=self.data, question_id=question_id
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
                data=self.data, question_id=question_id
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
            self.patient_id_data, self.processed_data)
        print("Done")

    def process_patient_age(self) -> pd.DataFrame:

        print("Adding patient_age column")
        self.processed_data = self.processed_data.merge(
            self.patient_age_data, left_index=True, right_on='wizyta_id', how='left')
        print("Done")
    
    def mapping_multiselect(self) -> pd.DataFrame:
        """mapping_multiselect mapuje odpowiedzi z sekcji detiled_interview oraz grupuje je ze względu na ta samą odpowiedź

        Returns
        -------
        pd.DataFrame
            self.processed_data po połączeniu tych samych odpowiedzi
        """
        print(f"Processing mapping multiselect answers...")
        self.processed_data = process_multiselect_mapping(
            self.processed_data)
        print(f"Done")

    def process_questions(
        self, remove_duplicate_questions: bool = True, calculating_bmi: bool = True, mapping_multiselect_questions: bool = True, subset: str = None, patient_id_index: bool = False
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
            #remove_duplicate_questions = True

        for question_id in set(self.usefull_questions).intersection(inter_set):
            print(f"Processing question {question_id}...")
            prepared_values = self.prepare_question(question_id)
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
            self.mapping_multiselect()
            print(
                f"Done. Number of columns after mapping= {self.processed_data.shape[1]}")
        if remove_duplicate_questions is True:
            self.process_duplicate_question()
            #self.merge_questions() TO DO FIX
        print(f"Final number of columns = {self.processed_data.shape[1]}")

        if patient_id_index is True:
            self.process_patient_id()
        
        self.process_patient_age()
        print("Done.")