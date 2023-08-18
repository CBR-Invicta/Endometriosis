from utils import DataSerieEDA
from cleaning import add_badanie_fizykalne_p1_characteristic, add_badanie_fizykalne_p1_numeric, clean_data, prepare_ushsg_dataset
from read_data import get_prescriptions, get_zalecenia_zlecenia, get_query, get_txt
import cleaning
from sqlalchemy import create_engine
import dotenv
import psycopg2
import os
import sys
import pandas as pd
import numpy as np
import base64
import datetime
from fuzzywuzzy import fuzz
import difflib
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List
import re
import json
import process_q_survey
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.getenv('lib_path'))


# czynnik jajowodowy
p1_columns_used = [
    'Objaw_lub_procedura:PCOS',
    'Objaw_lub_procedura:DRAINAGE_FALLOPIAN_TUBES',
    'Objaw_lub_procedura:DIABETES',
    '483_Czy_przeszła_Pani_zabieg_chirurgicznego_wycięcia_jednego_lub_obu_jajników?',
    'Objaw_lub_procedura:OVARIAN_CYSTS',
    'Objaw_lub_procedura:THYROID_PROBLEMS',
    '198_199_515_521_Czy_występują_w_Pani_rodzinie_jakieś_znane_Pani_choroby_genetyczne_lub_wady_wrodzone?',
    'Objaw_lub_procedura:ENDOMETRIOSIS'
]

def czynnik_jajowodowy(
    data: pd.DataFrame, wyniki_opis: pd.DataFrame, qualif_survey_results: pd.DataFrame
) -> pd.DataFrame:
    """Funkcja za pomocą, której dodawana jest kolumna z binarną oceną obecności czynnika niepłodności jajowodowej(jednostronnej i obustronnej)

    Logika dla czynnka jajowodowego:
    - stwierdzono w sono-HSG lub RT-HSG niedrożnośc jednego / obu jajowodów

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety

    Returns
    -------
    pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety oraz z określonym czynnikiem jajowodowym z opisów USG.
    """
    ushsg = cleaning.prepare_ushsg_dataset()
    wyniki_opis = wyniki_opis[["wizyta_id", "patient_id", "czynnik_jajowodowy"]].drop_duplicates()
    qualif_survey_results = qualif_survey_results[["patient_id", "ovarian_factor"]].drop_duplicates()

    ushsg = ushsg.merge(
        wyniki_opis[["wizyta_id", "patient_id", "czynnik_jajowodowy"]],
        on=["wizyta_id", "patient_id"],
        how="outer",
    ).drop_duplicates()
    ushsg = ushsg.merge(
        qualif_survey_results[["patient_id", "ovarian_factor"]],
        on="patient_id",
        how="outer",
    ).drop_duplicates()

    ushsg.czynnik_jajowodowy_jednostronny = np.where(
        ushsg.czynnik_jajowodowy_jednostronny == 1,
        1,
        np.where(
            ushsg.czynnik_jajowodowy == 1, 1, ushsg.czynnik_jajowodowy_jednostronny
        ),
    )

    ushsg["czynnik_jajowodowy_jednostronny"] = np.where(
        ushsg["patient_id"].isin(
            list(
                ushsg.patient_id[
                    (ushsg["czynnik_jajowodowy_jednostronny"] == 1)
                ].drop_duplicates()
            )
        ),
        1,
        0,
    )
    ushsg["czynnik_jajowodowy_obustronny"] = np.where(
        ushsg["patient_id"].isin(
            list(
                ushsg.patient_id[
                    (ushsg["czynnik_jajowodowy_obustronny"] == 1)
                ].drop_duplicates()
            )
        ),
        1,
        0,
    )

    czynniki_jajowodowe = ushsg.groupby("patient_id").agg(
        {
            "czynnik_jajowodowy_jednostronny": "max",
            "czynnik_jajowodowy_obustronny": "max",
            "ovarian_factor": "max",
        }
    )

    czynniki_jajowodowe["czynnik_jajowodowy_jednostronny"] = czynniki_jajowodowe[
        ["czynnik_jajowodowy_jednostronny", "ovarian_factor"]
    ].sum(axis=1)

    czynniki_jajowodowe["czynnik_jajowodowy_jednostronny"] = np.where(
        czynniki_jajowodowe["czynnik_jajowodowy_jednostronny"] > 0, 1, 0
    )
    czynniki_jajowodowe.drop(["ovarian_factor"], axis=1, inplace=True)
    czynniki_jajowodowe.reset_index(inplace=True)

    result = (
        data.reset_index()
        .merge(czynniki_jajowodowe, how="left", on="patient_id").drop_duplicates()
        .set_index("wizyta_id")
    )

    return result

# obniżona rezerwa jajnikowa


def obnizona_rezerwa_jajnikowa(
    data: pd.DataFrame,
    wyniki_badan: pd.DataFrame,
    daty_ankiet: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    norm_min_amh: int = 1,
    norm_min_afc: int = 6,
    norm_max_fsh: int = 12,
) -> pd.DataFrame:
    """Metoda przygotowująca tabelę z binarnymi wartościami obniżonej rezerwy jajnikowej (na podstawie wyników badań) razem z wynikami z ankiety pierwszorazowej

    Logika dla czynnika obniżona rezerwa jajnikowa:
    - AMH < 1 lub
    - AFC < 6 lub
    - FSH > 12

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    wyniki_badan : pd.DataFrame
        Tabela z wynikami badań laboratoryjnych kobiet

    Returns
    -------
    pd.DataFrame
        Połączone tabele z odpowiedziami na pytania z ankiety pierwszorazowej oraz binarna odpowiedz na czynnik obniżonej rezerwy jajnikowej
    """
    df = wyniki_badan.rename(columns={"id_wizyta": "wizyta_id"}).merge(
        daty_ankiet[["wizyta_id", "response_date"]], on="wizyta_id", how="left"
    )
    df = df.sort_values(["wizyta_id", "response_date"])
    df.drop_duplicates(
        subset=["wizyta_id", "analiza_id"], inplace=True, keep="last")

    df["diff"] = (df["result_time"] - df["response_date"]
                  ).astype("timedelta64[D]")

    wyniki_do_roku_AMH = df[(df["diff"] < 365) &
                            df["analiza_id"].isin([832, 1970])]
    wyniki_do_roku_FSH = df[(df["diff"] < 365) & df["analiza_id"].isin([713])]

    pacjentki_AMH_1Y_bad = list(
        wyniki_do_roku_AMH.patient_id_wizyta[wyniki_do_roku_AMH.result < norm_min_amh]
    )
    pacjentki_FSH_1Y_bad = list(
        wyniki_do_roku_FSH.patient_id_wizyta[wyniki_do_roku_FSH.result > norm_max_fsh]
    )

    pacjentki_AMH_1Y_good = list(
        wyniki_do_roku_AMH.patient_id_wizyta[wyniki_do_roku_AMH.result > norm_min_amh]
    )
    pacjentki_FSH_1Y_good = list(
        wyniki_do_roku_FSH.patient_id_wizyta[wyniki_do_roku_FSH.result < norm_max_fsh]
    )

    df["AMH"] = np.where(
        df["patient_id_wizyta"].isin(pacjentki_AMH_1Y_bad),
        1,
        (
            np.where(
                df["patient_id_wizyta"].isin(pacjentki_AMH_1Y_good),
                0,
                np.where(
                    ~df["patient_id_wizyta"].isin(
                        pacjentki_AMH_1Y_good + pacjentki_AMH_1Y_bad
                    )
                    & (df["analiza_id"].isin([832, 1970]) & (df.result < norm_min_amh)),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["FSH"] = np.where(
        df["patient_id_wizyta"].isin(pacjentki_FSH_1Y_bad),
        1,
        (
            np.where(
                df["patient_id_wizyta"].isin(pacjentki_FSH_1Y_good),
                0,
                np.where(
                    ~df["patient_id_wizyta"].isin(
                        pacjentki_FSH_1Y_good + pacjentki_FSH_1Y_bad
                    )
                    & (df["analiza_id"].isin([713]) & (df.result > norm_max_fsh)),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df = df.rename(columns={"patient_id_wizyta": "patient_id"})
    wyniki_badan_AMH_FSH = df.groupby(
        "patient_id").agg({"AMH": "max", "FSH": "max"})

    afc = cleaning.prepare_afc_dataset()
    afc = afc.merge(
        daty_ankiet[["wizyta_id", "response_date"]], on="wizyta_id", how="left"
    )
    afc = afc.sort_values(["wizyta_id", "response_date"])
    afc.drop_duplicates(subset=["wizyta_id"], inplace=True, keep="last")
    afc["diff"] = (afc["result_time"] - afc["response_date"]
                   ).astype("timedelta64[D]")

    pacjentki_AFC_1Y_bad = list(
        afc.patient_id[
            (afc["diff"] < 365) & (
                afc[["afc_jp", "afc_jl"]].sum(axis=1) < norm_min_afc)
        ]
    )
    pacjentki_AFC_1Y_good = list(
        afc.patient_id[
            (afc["diff"] < 365) & (
                afc[["afc_jp", "afc_jl"]].sum(axis=1) > norm_min_afc)
        ]
    )

    afc["AFC"] = np.where(
        afc["patient_id"].isin(pacjentki_AFC_1Y_bad),
        1,
        (
            np.where(
                afc["patient_id"].isin(pacjentki_AFC_1Y_good),
                0,
                np.where(
                    ~afc["patient_id"].isin(
                        pacjentki_AFC_1Y_good + pacjentki_AFC_1Y_bad
                    )
                    & (afc[["afc_jp", "afc_jl"]].sum(axis=1) < norm_min_afc),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    afc = afc.groupby("patient_id").agg({"AFC": "max"})

    low_res = pd.concat([wyniki_badan_AMH_FSH, afc["AFC"]], axis=1)

    low_res = low_res.merge(qualif_survey_results[["patient_id", "low_ovarian_reserve", "premature_expiration_ovaries", "hormone_antymuller7"]], on="patient_id", how="outer").drop_duplicates()
    low_res['hormone_antymuller7'] = (low_res.hormone_antymuller7==0)*1

    low_res["sum"] = low_res[["AMH", "FSH", "AFC", "low_ovarian_reserve", "premature_expiration_ovaries","hormone_antymuller7"]].sum(axis=1)
    low_res["obnizona_rezerwa_jajnikowa"] = np.where(
        (low_res["sum"] > 0), 1, 0)

    result = data.reset_index().merge(
        low_res[["obnizona_rezerwa_jajnikowa", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")

    return result

# policystyczne jajniki


def policystyczne_jajniki(
    data: pd.DataFrame,
    wyniki_badan: pd.DataFrame,
    wyniki_opis: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    # norm_min_amh: int = 1,
    norm_max_amh: int = 6,
    # norm_min_afc: int = 6,
    norm_max_afc: int = 36,
) -> pd.DataFrame:
    """Metoda przygotowująca tabelę z binarnymi wartościami policystycznych jajnikow (na podstawie wyników badań) razem z wynikami z ankiety pierwszorazowej


    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    wyniki_badan : pd.DataFrame
        Tabela z wynikami badań laboratoryjnych kobiet

    Returns
    -------
    pd.DataFrame
        Połączone tabele z odpowiedziami na pytania z ankiety pierwszorazowej oraz binarna odpowiedz na czynnik policystycznych jajnikow
    """
    wyniki_badan = wyniki_badan.rename(columns={"id_wizyta": "wizyta_id"})

    patient_id_AMH_bad = list(
        wyniki_badan.patient_id_wizyta[
            wyniki_badan["analiza_id"].isin([832, 1970]) & (
                wyniki_badan.result > norm_max_amh)
        ].drop_duplicates()
    )

    patient_id_AMH_good = list(
        wyniki_badan.patient_id_wizyta[
            wyniki_badan["analiza_id"].isin([832, 1970]) & (
                wyniki_badan.result < norm_max_amh)# & (wyniki_badan.result > norm_min_amh)
        ].drop_duplicates()
    )

    wyniki_badan["AMH"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_AMH_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_AMH_good), 0, np.NaN)
    )
    wyniki_badan = wyniki_badan.rename(
        columns={"patient_id_wizyta": "patient_id"})
    wyniki_badan_AMH = wyniki_badan.groupby("patient_id").agg({"AMH": "max"})

    afc = cleaning.prepare_afc_dataset()

    patient_id_AFC_bad = list(
        afc.patient_id[afc[["afc_jp", "afc_jl"]].sum(axis=1) > norm_max_afc].drop_duplicates())

    patient_id_AFC_good = list(
        afc.patient_id[(afc[["afc_jp", "afc_jl"]].sum(axis=1) < norm_max_afc)].drop_duplicates()) # & (afc[["afc_jp", "afc_jl"]].sum(axis=1) > norm_min_afc)

    afc["AFC"] = np.where(afc["patient_id"].isin(patient_id_AFC_bad), 1, np.where(
        afc["patient_id"].isin(patient_id_AFC_good), 0, np.NaN))

    afc = afc.groupby("patient_id").agg({"AFC": "max"})
    wyniki_opis = wyniki_opis.groupby("patient_id").agg(
        {"policystyczne_jajniki": "max"})

    pol = pd.concat([wyniki_badan_AMH, afc["AFC"],
                    wyniki_opis["policystyczne_jajniki"]], axis=1)

    pol = pol.merge(qualif_survey_results[["patient_id", "pco"]], on="patient_id", how="outer").drop_duplicates()

    pol["sum"] = pol[["AMH", "AFC", "policystyczne_jajniki", "pco"]].sum(axis=1)

    pol["policystyczne_jajniki"] = np.where(
        (pol["sum"] > 0), 1, np.where((pol["sum"] == 0), 0, np.NaN))

    result = data.reset_index().merge(
        pol[["policystyczne_jajniki", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")
    return result

# zaburzenia glikemii


def zaburzenia_glikemii(data: pd.DataFrame, wyniki_opis: pd.DataFrame) -> pd.DataFrame:
    """Funkcja twojrzaca tabelę binarną czynnika zaburzenia glikemii oraz łącząca wspomnianą tabelę z odpowiedziami na pytania w ankiecie pierwszorazowej

    Logika dla czynnika zaburzenia glikemii:
    - glukoza na czczo >125 mg/dl
    -cukrzyca ze zrodła wyniki_opis

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej

    Returns
    -------
    pd.DataFrame
        Tabela z binarnymi odpowiedziami na pytania w ankiecie oraz z kolumną dotyczącą zaburzeń glikemii na podstawie badań laboratoryjnych
    """
    glukoza = cleaning.prepare_podwyzszona_glukoza_dataset()
    glukoza = glukoza.rename(
        columns={"id_wizyta": "wizyta_id",}
    )
    glukoza = glukoza.merge(wyniki_opis[["wizyta_id", "cukrzyca"]], on=[
                            "wizyta_id"], how="outer").drop_duplicates()

    glukoza["zaburzenia_glikemii"] = np.where(glukoza[["podwyzszona_glukoza", "cukrzyca"]].sum(axis=1) > 0, 1, 0)

    glukoza = glukoza.rename(columns={"patient_id_wizyta": "patient_id"})
    zaburzenia_glikemii = glukoza.groupby("patient_id").agg({"zaburzenia_glikemii":"max"})
    zaburzenia_glikemii.reset_index(inplace=True)

    result = data.reset_index().merge(
        zaburzenia_glikemii[["zaburzenia_glikemii", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")
        
    return result

# nieprawidłowości w obrębie macicy


def nieprawidlowosci_w_obrebie_macicy(data: pd.DataFrame, zmiany_macicy,  wyniki_opis: pd.DataFrame, zmiany_macicy_opis: List = ["zrosty", "miesniak", "polip"]) -> pd.DataFrame:
    """Funkcja tworząca tabelę z binarnym czynnikiem nieprwidlowosci w obrebie macicy na podstawie wyników laboratoryjnych

    Logika dla czynnika nieprawidłowości w obrębie macicy:
    -Polipy
    - Zrosty
    - Mięśniaki

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    zmiany_macicy : pd.DataFrame
        Wyniki badań laboratoryjnych pochodzące z opisów badań USG

    Returns
    -------
    pd.DataFrame
        Tabela z połączonymi odpowiedziami z ankiet pierwszorazowych razem z stwierdzeniem wystepowania czynnika nieprawidlowosci w obrebie macicy.
    """
    zmiany = zmiany_macicy.copy()
    zmiany.reset_index(inplace=True)
    zmiany = zmiany.merge(wyniki_opis[["patient_id", "wizyta_id", "zrosty",	"miesniak",	"polip"]], on=[
                          "patient_id", "wizyta_id", "zrosty",	"miesniak",	"polip"], how="outer").drop_duplicates()
    zmiany = zmiany.sort_values(
        ["wizyta_id", "zrosty",	"miesniak",	"polip"], ascending=False)
    zmiany = zmiany.drop_duplicates(
        subset=["patient_id", "wizyta_id"], keep="first")

    patient_id_zrosty = list(
        zmiany.patient_id[(
            zmiany["zrosty"] == 1)].drop_duplicates()
    )
    patient_id_miesniak = list(
        zmiany.patient_id[(
            zmiany["miesniak"] == 1)].drop_duplicates()
    )
    patient_id_polip = list(
        zmiany.patient_id[(
            zmiany["polip"] == 1)].drop_duplicates()
    )

    zmiany["zrosty"] = np.where(
        zmiany["patient_id"].isin(patient_id_zrosty), 1, 0
    )
    zmiany["miesniak"] = np.where(
        zmiany["patient_id"].isin(patient_id_miesniak), 1, 0
    )
    zmiany["polip"] = np.where(
        zmiany["patient_id"].isin(patient_id_polip), 1, 0
    )

    zmiany["sum"] = zmiany[zmiany_macicy_opis].sum(axis=1)
    zmiany["nieprawidlowosci_w_obrebie_macicy"] = np.where(
        zmiany["sum"] > 0, 1, 0
    )

    zmiany_w_obrebie_macicy = zmiany.groupby("patient_id").agg(
        {"nieprawidlowosci_w_obrebie_macicy": "max"})
    zmiany_w_obrebie_macicy.reset_index(inplace=True)

    result = data.reset_index().merge(zmiany_w_obrebie_macicy, how="left",
                                      on="patient_id").set_index("wizyta_id")
    return result

# zaburzenia budowy jamy macicy

def zaburzenia_budowy_jamy_macicy(
    data: pd.DataFrame,
    zmiany_macicy: pd.DataFrame,
    wyniki_opis: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    zmiany_macicy_opis: List = [
        "jednorozn",
        "dwurozn",
        "przegrod",
        "niejednorodn",
        "ektopi",
    ],
) -> pd.DataFrame:
    """Funkcja przygotowująca tabelę z binarnymi odpowiedziami na czynnik zaburzenia budowy jamy macicy na podstawie opisów badań USG
    oraz łączy ją z tabelą z przeprocesowanymi pytaniami z ankiety pierwszorazowej.

    Logika dla czynnika zaburzenia budowy jamy macicy:

    - Macica jednorożna
    - Macica dwurożna
    - Macica z przegrodą
    - Macica podwójna
    - Inne zaburzenia budowy jamy macicy (ektopia)

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    zmiany_macicy : pd.DataFrame
        Tabela z wynikami USG macicy

    Returns
    -------
    pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej i czynnikiem zaburzenia budowy jamy macicy
    """
    zmiany = zmiany_macicy.copy()
    zmiany.reset_index(inplace=True)
    zmiany = zmiany.merge(wyniki_opis[["patient_id", "wizyta_id", "jednorozn",	"dwurozn",	"przegrod", "niejednorodn"]], on=[
                          "patient_id", "wizyta_id", "jednorozn",	"dwurozn",	"przegrod", "niejednorodn"], how="outer").drop_duplicates()
    zmiany = zmiany.sort_values(
        ["wizyta_id", "jednorozn",	"dwurozn",	"przegrod", "niejednorodn"], ascending=False)
    zmiany = zmiany.drop_duplicates(
        subset=["patient_id", "wizyta_id"], keep="first")

    patient_id_jednorozn = list(
        zmiany.patient_id[(
            zmiany["jednorozn"] == 1)].drop_duplicates()
    )
    patient_id_dwurozn = list(
        zmiany.patient_id[(
            zmiany["dwurozn"] == 1)].drop_duplicates()
    )
    patient_id_przegrod = list(
        zmiany.patient_id[(
            zmiany["przegrod"] == 1)].drop_duplicates()
    )
    patient_id_niejednorodn = list(
        zmiany.patient_id[(
            zmiany["niejednorodn"] == 1)].drop_duplicates()
    )
    patient_id_ektopi = list(
        zmiany.patient_id[(
            zmiany["ektopi"] == 1)].drop_duplicates()
    )

    zmiany["jednorozn"] = np.where(
        zmiany["patient_id"].isin(patient_id_jednorozn), 1, 0
    )
    zmiany["dwurozn"] = np.where(
        zmiany["patient_id"].isin(patient_id_dwurozn), 1, 0
    )
    zmiany["przegrod"] = np.where(
        zmiany["patient_id"].isin(patient_id_przegrod), 1, 0
    )
    zmiany["niejednorodn"] = np.where(
        zmiany["patient_id"].isin(patient_id_niejednorodn), 1, 0
    )
    zmiany["ektopi"] = np.where(
        zmiany["patient_id"].isin(patient_id_ektopi), 1, 0
    )

    zmiany = zmiany.merge(qualif_survey_results[["patient_id", "endometrium_disorder", "uterus_loss"]], on="patient_id", how="outer").drop_duplicates()

    zmiany["sum"] = zmiany[zmiany_macicy_opis + ["endometrium_disorder", "uterus_loss"]].sum(axis=1)
    zmiany["zaburzenia_budowy_jamy_macicy"] = np.where(
        zmiany["sum"] > 0, 1, 0
    )

    zmiany_budowy_jamy_macicy = zmiany.groupby(
        "patient_id").agg({"zaburzenia_budowy_jamy_macicy": "max"})
    zmiany_budowy_jamy_macicy.reset_index(inplace=True)

    result = data.reset_index().merge(zmiany_budowy_jamy_macicy, how="left",
                                      on="patient_id").set_index("wizyta_id")
    return result

# nieprawidlowosci w obrebie jajników


def nieprawidlowosci_w_obrebie_jajnikow(
    data: pd.DataFrame,
    zmiany_macicy: pd.DataFrame,
    wyniki_opis: pd.DataFrame,
    zmiany_macicy_opis: List = ["torbiel"],
) -> pd.DataFrame:
    """Funkcja przygotowująca tabelę z binarnymi odpowiedziami na czynnik nieprawidlowosci w obrebie jajnikow na podstawie opisów badań USG
    oraz łączy ją z tabelą z przeprocesowanymi pytaniami z ankiety pierwszorazowej.

    Logika dla czynnika nieprawidlowosci w obrebie jajnikow:
    - torbiel

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    zmiany_macicy : pd.DataFrame
        Tabela z wynikami USG jajnikow

    Returns
    -------
    pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej i czynnikiem nieprawidlowosci w obrebie jajnikow
    """
    zmiany = zmiany_macicy.copy()
    zmiany.reset_index(inplace=True)
    zmiany = zmiany.merge(wyniki_opis[["patient_id", "wizyta_id", "torbiel"]], on=[
                          "patient_id", "wizyta_id", "torbiel"], how="outer")
    zmiany = zmiany.sort_values(["wizyta_id", "torbiel"], ascending=False)
    zmiany = zmiany.drop_duplicates(
        subset=["patient_id", "wizyta_id"], keep="first")

    patient_id_torbiel = list(
        zmiany.patient_id[(
            zmiany["torbiel"] == 1)].drop_duplicates()
    )

    zmiany["torbiel"] = np.where(
        zmiany["patient_id"].isin(patient_id_torbiel), 1, 0
    )

    zmiany["sum"] = zmiany[zmiany_macicy_opis].sum(axis=1)

    zmiany["nieprawidlowosci_w_obrebie_jajnikow"] = np.where(
        zmiany["sum"] > 0, 1, 0
    )

    nieprawidlowosci_w_obrebie_jajnikow = zmiany.groupby(
        "patient_id").agg({"nieprawidlowosci_w_obrebie_jajnikow": "max"})
    nieprawidlowosci_w_obrebie_jajnikow.reset_index(inplace=True)

    result = data.merge(nieprawidlowosci_w_obrebie_jajnikow,
                        how="left", left_index=True, right_index=True).drop_duplicates()
    result = data.reset_index().merge(nieprawidlowosci_w_obrebie_jajnikow,
                                      how="left", on="patient_id").set_index("wizyta_id")

    return result

# obniżone parametry nasienia


def obnizone_parametry_nasienia(
    data: pd.DataFrame,
    wyniki_men: pd.DataFrame,
    daty_ankiet: pd.DataFrame,
    wyniki_opis: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    sex_id : str,
    norm_min_objetosc_nasienia: int = 1.5,
    norm_min_ph: int = 7.2,
    norm_max_czas_uplynnienia: int = 60,
    norm_min_liczba_plemnikow_ejakulat: int = 39,
    norm_min_ruch_postepowy: int = 30,
    norm_max_fragmentacja_plemnikow: int = 15,
    norm_min_wiazanie_hialuronian: int = 80,
    norm_max_potencjal_oksyd_reduk: int = 1.42,
    norm_sposob_uplynnienia: int = 0,
    norm_min_zywotnosc_plemnikow: int = 58,
    norm_min_gestosc_plemnikow: int = 16,
    norm_min_leukocyty: int = 1,
    norm_min_prawidlowa_budowa: int = 4,
    norm_max_martwe: int = 89,
) -> pd.DataFrame:
    """Funkcja przygotowujaca kolumne z czynnikiem obnizone parametry nasienia i lącząca ja z tabelą z przeprocesowanymi pytaniami z ankiety pierwszorazowej

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    wyniki_men : pd.DataFrame
        Wyniki badań laboratoryjnych mężczyzn
    norm_min_objetosc_nasienia : int, optional
        min. wartosc zakresu referencyjnego dla objętości nasienia, by default 1.5
    norm_min_ph : int, optional
        min. wartosc zakresu referencyjnego dla pH nasienia, by default 7.2
    norm_max_czas_uplynnienia : int, optional
        max. wartosc zakresu referencyjnego dla czasu upłynnienia nasienia, by default 60
    norm_min_liczba_plemnikow_ejakulat : int, optional
        min. wartosc zakresu referencyjnego dla liczby plemników w ejakulacie, by default 39
    norm_min_ruch_postepowy : int, optional
        min. wartosc zakresu referencyjnego dla sumy plemników o ruchu postepowym, by default 30
    norm_max_fragmentacja_plemnikow : int, optional
        max. wartosc zakresu referencyjnego dla stopnia fragmentacji plemników, by default 15
    norm_min_wiazanie_hialuronian : int, optional
        min. wartosc zakresu referencyjnego dla stopnia wiązania hialuronianu, by default 80
    norm_max_potencjal_oksyd_reduk : int, optional
        max. wartosc zakresu referencyjnego dla stopnia potencjału oksydacyjno-redukcyjnego, by default 1.42
    norm_sposob_uplynnienia : int, optional
        Wartość referencyjna dla sposobu upłynnienia ( 0 - samoistne, 1-mechaniczne, 2- enzymatyczne), by default 0
    norm_min_zywotnosc_plemnikow : int, optional
        min. wartosc zakresu referencyjnego dla zywotnosci plemnikow, by default 58
    norm_min_gestosc_plemnikow : int, optional
        min. wartosc zakresu referencyjnego dla gęstości plemnikow, by default 16

    Returns
    -------
    pd.DataFrame
        Tabela, która łączy odpowiedzi na przeprocesowane pytania z ankiety pierwszorazowej oraz czynnik obniżone parametry nasienia
    """
    wyniki_men = wyniki_men.rename(columns={"id_wizyta": "wizyta_id"})
    
    wyniki_men_AB = wyniki_men[wyniki_men["analiza_id"].isin([602, 603])]
    wyniki_men_AB = wyniki_men_AB[[sex_id,'wizyta_id', 'result']].groupby([sex_id,'wizyta_id']).sum().reset_index()

    df = wyniki_men.merge(
        daty_ankiet[["wizyta_id", "response_date"]], on="wizyta_id", how="left"
    )
    df = df.sort_values(["wizyta_id", "response_date"])
    df.drop_duplicates(
        subset=["wizyta_id", "analiza_id"], inplace=True, keep="last")

    df["diff"] = (df["result_time"] - df["response_date"]
                  ).astype("timedelta64[D]")

    regex = re.compile(r"(\baglutynacja\b)", re.IGNORECASE)

    df["aglut"] = df.loc[df.analiza_id.isin(
        [805, 955])].original_result.str.extract(regex, expand=True)
    df["aglut"] = np.where(df["aglut"].notna(), 1, np.NaN)

    wyniki_do_roku_aglutynacja = df[
        (df["diff"] < 365) & df["analiza_id"].isin([805, 955])
    ]

    wyniki_do_roku_objetosc_nasienia = df[
        (df["diff"] < 365) & df["analiza_id"].isin([599])
    ]
    wyniki_do_roku_ph_nasienia = df[(
        df["diff"] < 365) & df["analiza_id"].isin([600])]
    wyniki_do_roku_czas_uplynnienia = df[
        (df["diff"] < 365) & df["analiza_id"].isin([596])
    ]
    wyniki_do_roku_liczba_plemnikow_w_ejakulacie = df[
        (df["diff"] < 365) & df["analiza_id"].isin([611])
    ]
    wyniki_do_roku_suma_plemnikow_ruch_postepowy = df[
        (df["diff"] < 365) & df["analiza_id"].isin([[2310, 1032]])
    ]
    wyniki_do_roku_fragmentacja_plemników = df[
        (df["diff"] < 365) & df["analiza_id"].isin([[2275, 701]])
    ]
    wyniki_do_roku_test_wiazania_z_hialuronianem = df[
        (df["diff"] < 365) & df["analiza_id"].isin([1803])
    ]
    wyniki_do_roku_potencjal_oksydacyjno_redukcyjny = df[
        (df["diff"] < 365) & df["analiza_id"].isin([2215])
    ]
    wyniki_do_roku_sposob_uplynnienia = df[
        (df["diff"] < 365) & df["analiza_id"].isin([799])
    ]
    wyniki_do_roku_zywotnosc_plemnikow = df[
        (df["diff"] < 365) & df["analiza_id"].isin([614, 2308])
    ]
    wyniki_do_roku_gestosc_plemnikow = df[
        (df["diff"] < 365) & df["analiza_id"].isin([610])
    ]
    wyniki_do_roku_leukocyty = df[
        (df["diff"] < 365) & df["analiza_id"].isin([803])
    ]
    wyniki_do_roku_prawidlowa_budowa = df[
        (df["diff"] < 365) & df["analiza_id"].isin([1083])
    ]

    wyniki_do_roku_martwe_plemniki = df[
        (df["diff"] < 365) & df["analiza_id"].isin([613, 2309])
    ]

    wyniki_do_roku_D = df[
        (df["diff"] < 365) & df["analiza_id"].isin([605])
    ]

    wyniki_do_roku_C = df[
        (df["diff"] < 365) & df["analiza_id"].isin([608])
    ]

    pacjentki_aglutynacja_1Y_bad = list(
        wyniki_do_roku_aglutynacja[sex_id][
            wyniki_do_roku_aglutynacja.aglut == 1
        ]
    )
    pacjentki_objetosc_nasienia_1Y_bad = list(
        wyniki_do_roku_objetosc_nasienia[sex_id][
            wyniki_do_roku_objetosc_nasienia.result < norm_min_objetosc_nasienia
        ]
    )
    pacjentki_ph_nasienia_1Y_bad = list(
        wyniki_do_roku_ph_nasienia[sex_id][
            wyniki_do_roku_ph_nasienia.result < norm_min_ph
        ]
    )
    pacjentki_czas_uplynnienia_1Y_bad = list(
        wyniki_do_roku_czas_uplynnienia[sex_id][
            wyniki_do_roku_czas_uplynnienia.result > norm_max_czas_uplynnienia
        ]
    )
    pacjentki_liczba_plemnikow_w_ejakulacie_1Y_bad = list(
        wyniki_do_roku_liczba_plemnikow_w_ejakulacie[sex_id][
            wyniki_do_roku_liczba_plemnikow_w_ejakulacie.result
            < norm_min_liczba_plemnikow_ejakulat
        ]
    )
    pacjentki_suma_plemnikow_ruch_postepowy_1Y_bad = list(
        wyniki_do_roku_suma_plemnikow_ruch_postepowy[sex_id][
            wyniki_do_roku_suma_plemnikow_ruch_postepowy.result
            < norm_min_ruch_postepowy
        ]
    )
    pacjentki_fragmentacja_plemników_1Y_bad = list(
        wyniki_do_roku_fragmentacja_plemników[sex_id][
            wyniki_do_roku_fragmentacja_plemników.result
            > norm_max_fragmentacja_plemnikow
        ]
    )
    pacjentki_test_wiazania_z_hialuronianem_1Y_bad = list(
        wyniki_do_roku_test_wiazania_z_hialuronianem[sex_id][
            wyniki_do_roku_test_wiazania_z_hialuronianem.result
            < norm_min_wiazanie_hialuronian
        ]
    )
    pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_bad = list(
        wyniki_do_roku_potencjal_oksydacyjno_redukcyjny[sex_id][
            wyniki_do_roku_potencjal_oksydacyjno_redukcyjny.result
            > norm_max_potencjal_oksyd_reduk
        ]
    )
    pacjentki_sposob_uplynnienia_1Y_bad = list(
        wyniki_do_roku_sposob_uplynnienia[sex_id][
            wyniki_do_roku_sposob_uplynnienia.result > norm_sposob_uplynnienia
        ]
    )
    pacjentki_zywotnosc_plemnikow_1Y_bad = list(
        wyniki_do_roku_zywotnosc_plemnikow[sex_id][
            wyniki_do_roku_zywotnosc_plemnikow.result < norm_min_zywotnosc_plemnikow
        ]
    )
    pacjentki_gestosc_plemnikow_1Y_bad = list(
        wyniki_do_roku_gestosc_plemnikow[sex_id][
            wyniki_do_roku_gestosc_plemnikow.result < norm_min_gestosc_plemnikow
        ]
    )
    pacjentki_leukocyty_1Y_bad = list(
        wyniki_do_roku_leukocyty[sex_id][
            wyniki_do_roku_leukocyty.result < norm_min_leukocyty
        ]
    )

    pacjentki_prawidlowa_budowa_1Y_bad = list(
        wyniki_do_roku_prawidlowa_budowa[sex_id][
            wyniki_do_roku_prawidlowa_budowa.result < norm_min_prawidlowa_budowa
        ]
    )

    pacjentki_martwe_plemniki_1Y_bad = list(
        wyniki_do_roku_martwe_plemniki[sex_id][
            wyniki_do_roku_martwe_plemniki.result > norm_max_martwe
        ]
    )
    pacjentki_AB_bad = wyniki_men_AB[sex_id][(wyniki_men_AB["result"] < 32)]

    pacjentki_AB_good = wyniki_men_AB[sex_id][(wyniki_men_AB["result"] > 32)]

    pacjentki_D_1Y_good = list(
        wyniki_do_roku_D[sex_id][
            wyniki_do_roku_D.result < 60
        ]
    )
    pacjentki_D_1Y_bad = list(
        wyniki_do_roku_D[sex_id][
            wyniki_do_roku_D.result > 60
        ]
    )

    pacjentki_C_1Y_good = list(
        wyniki_do_roku_C[sex_id][
            wyniki_do_roku_C.result < 60
        ]
    )
    pacjentki_C_1Y_bad = list(
        wyniki_do_roku_C[sex_id][
            wyniki_do_roku_C.result > 60
        ]
    )

    pacjentki_aglutynacja_1Y_good = list(
        wyniki_do_roku_aglutynacja[sex_id][
            wyniki_do_roku_aglutynacja.aglut != 1
        ]
    )

    pacjentki_objetosc_nasienia_1Y_good = list(
        wyniki_do_roku_objetosc_nasienia[sex_id][
            wyniki_do_roku_objetosc_nasienia.result > norm_min_objetosc_nasienia
        ]
    )
    pacjentki_ph_nasienia_1Y_good = list(
        wyniki_do_roku_ph_nasienia[sex_id][
            wyniki_do_roku_ph_nasienia.result > norm_min_ph
        ]
    )
    pacjentki_czas_uplynnienia_1Y_good = list(
        wyniki_do_roku_czas_uplynnienia[sex_id][
            wyniki_do_roku_czas_uplynnienia.result < norm_max_czas_uplynnienia
        ]
    )
    pacjentki_liczba_plemnikow_w_ejakulacie_1Y_good = list(
        wyniki_do_roku_liczba_plemnikow_w_ejakulacie[sex_id][
            wyniki_do_roku_liczba_plemnikow_w_ejakulacie.result
            > norm_min_liczba_plemnikow_ejakulat
        ]
    )
    pacjentki_suma_plemnikow_ruch_postepowy_1Y_good = list(
        wyniki_do_roku_suma_plemnikow_ruch_postepowy[sex_id][
            wyniki_do_roku_suma_plemnikow_ruch_postepowy.result
            > norm_min_ruch_postepowy
        ]
    )
    pacjentki_fragmentacja_plemników_1Y_good = list(
        wyniki_do_roku_fragmentacja_plemników[sex_id][
            wyniki_do_roku_fragmentacja_plemników.result
            < norm_max_fragmentacja_plemnikow
        ]
    )
    pacjentki_test_wiazania_z_hialuronianem_1Y_good = list(
        wyniki_do_roku_test_wiazania_z_hialuronianem[sex_id][
            wyniki_do_roku_test_wiazania_z_hialuronianem.result
            > norm_min_wiazanie_hialuronian
        ]
    )
    pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_good = list(
        wyniki_do_roku_potencjal_oksydacyjno_redukcyjny[sex_id][
            wyniki_do_roku_potencjal_oksydacyjno_redukcyjny.result
            < norm_max_potencjal_oksyd_reduk
        ]
    )
    pacjentki_sposob_uplynnienia_1Y_good = list(
        wyniki_do_roku_sposob_uplynnienia[sex_id][
            wyniki_do_roku_sposob_uplynnienia.result == norm_sposob_uplynnienia
        ]
    )
    pacjentki_zywotnosc_plemnikow_1Y_good = list(
        wyniki_do_roku_zywotnosc_plemnikow[sex_id][
            wyniki_do_roku_zywotnosc_plemnikow.result > norm_min_zywotnosc_plemnikow
        ]
    )
    pacjentki_gestosc_plemnikow_1Y_good = list(
        wyniki_do_roku_gestosc_plemnikow[sex_id][
            wyniki_do_roku_gestosc_plemnikow.result > norm_min_gestosc_plemnikow
        ]
    )

    pacjentki_leukocyty_1Y_good = list(
        wyniki_do_roku_leukocyty[sex_id][
            wyniki_do_roku_leukocyty.result > norm_min_leukocyty
        ]
    )

    pacjentki_prawidlowa_budowa_1Y_good = list(
        wyniki_do_roku_prawidlowa_budowa[sex_id][
            wyniki_do_roku_prawidlowa_budowa.result > norm_min_prawidlowa_budowa
        ]
    )

    pacjentki_martwe_plemniki_1Y_good = list(
        wyniki_do_roku_martwe_plemniki[sex_id][
            wyniki_do_roku_martwe_plemniki.result < norm_max_martwe
        ]
    )

    df["aglutynacja"] = np.where(
        df[sex_id].isin(pacjentki_aglutynacja_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(pacjentki_aglutynacja_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_aglutynacja_1Y_good
                        + pacjentki_aglutynacja_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([805, 955])
                        & (df.result == 1)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["objetosc_nasienia"] = np.where(
        df[sex_id].isin(pacjentki_objetosc_nasienia_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_objetosc_nasienia_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_objetosc_nasienia_1Y_good
                        + pacjentki_objetosc_nasienia_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([599])
                        & (df.result < norm_min_objetosc_nasienia)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["ph_nasienia"] = np.where(
        df[sex_id].isin(pacjentki_ph_nasienia_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(pacjentki_ph_nasienia_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_ph_nasienia_1Y_good + pacjentki_ph_nasienia_1Y_bad
                    )
                    & (df["analiza_id"].isin([600]) & (df.result < norm_min_ph)),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["czas_uplynnienia"] = np.where(
        df[sex_id].isin(pacjentki_czas_uplynnienia_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_czas_uplynnienia_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_czas_uplynnienia_1Y_good
                        + pacjentki_czas_uplynnienia_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([596])
                        & (df.result > norm_max_czas_uplynnienia)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["liczba_plemnikow_w_ejakulacie"] = np.where(
        df[sex_id].isin(
            pacjentki_liczba_plemnikow_w_ejakulacie_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_liczba_plemnikow_w_ejakulacie_1Y_good
                ),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_liczba_plemnikow_w_ejakulacie_1Y_good
                        + pacjentki_liczba_plemnikow_w_ejakulacie_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([611])
                        & (df.result < norm_min_liczba_plemnikow_ejakulat)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["suma_plemnikow_ruch_postepowy"] = np.where(
        df[sex_id].isin(
            pacjentki_suma_plemnikow_ruch_postepowy_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_suma_plemnikow_ruch_postepowy_1Y_good
                ),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_suma_plemnikow_ruch_postepowy_1Y_good
                        + pacjentki_suma_plemnikow_ruch_postepowy_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([[2310, 1032]])
                        & (df.result < norm_min_ruch_postepowy)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["fragmentacja_plemników"] = np.where(
        df[sex_id].isin(pacjentki_fragmentacja_plemników_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_fragmentacja_plemników_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_fragmentacja_plemników_1Y_good
                        + pacjentki_fragmentacja_plemników_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([[2275, 701]])
                        & (df.result > norm_max_fragmentacja_plemnikow)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["test_wiazania_z_hialuronianem"] = np.where(
        df[sex_id].isin(
            pacjentki_test_wiazania_z_hialuronianem_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_test_wiazania_z_hialuronianem_1Y_good
                ),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_test_wiazania_z_hialuronianem_1Y_good
                        + pacjentki_test_wiazania_z_hialuronianem_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([1803])
                        & (df.result < norm_min_wiazanie_hialuronian)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["potencjal_oksydacyjno_redukcyjny"] = np.where(
        df[sex_id].isin(
            pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_good
                ),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_good
                        + pacjentki_potencjal_oksydacyjno_redukcyjny_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([2215])
                        & (df.result < norm_min_wiazanie_hialuronian)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["sposob_uplynnienia"] = np.where(
        df[sex_id].isin(pacjentki_sposob_uplynnienia_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_sposob_uplynnienia_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_sposob_uplynnienia_1Y_good
                        + pacjentki_sposob_uplynnienia_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([799])
                        & (df.result > norm_sposob_uplynnienia)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["zywotnosc_plemnikow"] = np.where(
        df[sex_id].isin(pacjentki_zywotnosc_plemnikow_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_zywotnosc_plemnikow_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_zywotnosc_plemnikow_1Y_good
                        + pacjentki_zywotnosc_plemnikow_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([614, 2308])
                        & (df.result < norm_min_zywotnosc_plemnikow)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["gestosc_plemnikow"] = np.where(
        df[sex_id].isin(pacjentki_gestosc_plemnikow_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_gestosc_plemnikow_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_gestosc_plemnikow_1Y_good
                        + pacjentki_gestosc_plemnikow_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([610])
                        & (df.result < norm_min_gestosc_plemnikow)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["leukocyty"] = np.where(
        df[sex_id].isin(pacjentki_leukocyty_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(pacjentki_leukocyty_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_leukocyty_1Y_good
                        + pacjentki_leukocyty_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([803])
                        & (df.result < norm_min_leukocyty)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["prawidlowa_budowa"] = np.where(
        df[sex_id].isin(pacjentki_prawidlowa_budowa_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_prawidlowa_budowa_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_prawidlowa_budowa_1Y_good
                        + pacjentki_prawidlowa_budowa_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([803])
                        & (df.result < norm_min_prawidlowa_budowa)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["martwe_plemniki"] = np.where(
        df[sex_id].isin(pacjentki_martwe_plemniki_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_martwe_plemniki_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_martwe_plemniki_1Y_good
                        + pacjentki_martwe_plemniki_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([613, 2309])
                        & (df.result > norm_max_martwe)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["AB_ruch"] = df["patient_id_wizyta"].isin(pacjentki_AB_bad)*1
    
    df["D_ruch"] = np.where(
        df[sex_id].isin(pacjentki_D_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_D_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_D_1Y_good
                        + pacjentki_D_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([605])
                        & (df.result > 60)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df["C_ruch"] = np.where(
        df[sex_id].isin(pacjentki_C_1Y_bad),
        1,
        (
            np.where(
                df[sex_id].isin(
                    pacjentki_C_1Y_good),
                0,
                np.where(
                    ~df[sex_id].isin(
                        pacjentki_C_1Y_good
                        + pacjentki_C_1Y_bad
                    )
                    & (
                        df["analiza_id"].isin([608])
                        & (df.result > 60)
                    ),
                    1,
                    np.NaN,
                ),
            )
        ),
    )

    df = df.rename(columns={sex_id: "patient_id"})

    wyniki_menz = df.groupby("patient_id").agg(
        {
            "aglutynacja": "sum",
            "objetosc_nasienia": "sum",
            "ph_nasienia": "sum",
            "czas_uplynnienia": "sum",
            "liczba_plemnikow_w_ejakulacie": "sum",
            "suma_plemnikow_ruch_postepowy": "sum",
            "fragmentacja_plemników": "sum",
            "test_wiazania_z_hialuronianem": "sum",
            "potencjal_oksydacyjno_redukcyjny": "sum",
            "sposob_uplynnienia": "sum",
            "zywotnosc_plemnikow": "sum",
            "gestosc_plemnikow": "sum",
            "leukocyty": "sum",
            "prawidlowa_budowa": "sum",
            "martwe_plemniki": "sum",
            "AB_ruch": "sum",
            "D_ruch": "sum",
            "C_ruch": "sum"
        }
    )

    wyniki_menz.reset_index(inplace=True)
    wyniki_menz = wyniki_menz.merge(wyniki_opis[["czynnik_meski", "patient_id"]], on=[
                                    "patient_id"], how="outer").drop_duplicates()
    wyniki_menz = wyniki_menz.merge(qualif_survey_results[["patient_id", "no_alive_sperm", "no_wiggling_sperm", "no_liquid_sperm", "no_sperm_in_testicle", "azoospermy", "low_hialuron", "high_oxydation_stres"]], on="patient_id", how="outer").drop_duplicates()


    wyniki_menz["sum"] = wyniki_menz[["aglutynacja",
                                    "objetosc_nasienia",
                                    "ph_nasienia",
                                    "czas_uplynnienia",
                                    "liczba_plemnikow_w_ejakulacie",
                                    "suma_plemnikow_ruch_postepowy",
                                    "fragmentacja_plemników",
                                    "test_wiazania_z_hialuronianem",
                                    "potencjal_oksydacyjno_redukcyjny",
                                    "sposob_uplynnienia",
                                    "zywotnosc_plemnikow",
                                    "gestosc_plemnikow",
                                    "leukocyty",
                                    "prawidlowa_budowa", 
                                    "martwe_plemniki",
                                    "AB_ruch",
                                    "D_ruch",
                                    "C_ruch",
                                    "czynnik_meski",
                                    "no_alive_sperm", 
                                    "no_wiggling_sperm", 
                                    "no_liquid_sperm", 
                                    "no_sperm_in_testicle", 
                                    "azoospermy", 
                                    "low_hialuron", 
                                    "high_oxydation_stres"]].sum(axis=1)

    wyniki_menz["obnizone_parametry_nasienia"] = np.where(
        wyniki_menz["sum"] > 0, 1, np.where((wyniki_menz["sum"] == 0), 0, np.NaN))

    result = data.reset_index().merge(
        wyniki_menz[["obnizone_parametry_nasienia", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")

    return result

# czynnik męski


def czynnik_meski(
    data: pd.DataFrame, wyniki_men: pd.DataFrame, wyniki_opis: pd.DataFrame, qualif_survey_results: pd.DataFrame, sex_id : str, norm_min_density: int = 16
) -> pd.DataFrame:
    """Funkcja przygotowująca tabelę z odpowiedziami binarnymi na obecność czynnika męskiego i łącząca ja z odpowiedziami na pytania z ankiety pierwszorazowej

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych z przeprocesowanymi pytaniami z ankiety pierwszorazowej
    wyniki_men : pd.DataFrame
        Zbiór danych z wynikami laboratoryjnymi wyników mężczyzny
    norm_min_density : int, optional
        min. wartość referencyjna dla gęstości nasienia, by default 16

    Returns
    -------
    pd.DataFrame
        Zbiór danych łączący czynnik męski z odpowiedziami na pytania z ankiety pierwszorazowej.
    """
    patient_id_brak_ruchomych_plemnikow = list(
        wyniki_men[sex_id][
            (wyniki_men["analiza_id"].isin([2310, 1032])
             & (wyniki_men["result"] < 5))
        ].drop_duplicates()
    )

    patient_id_brak_żywych_plemnikow = list(
        wyniki_men[sex_id][
            (wyniki_men["analiza_id"].isin([2309, 613])
             & (wyniki_men["result"] > 89))
        ].drop_duplicates()
    )

    patient_id_gestosc_plemnikow = list(
        wyniki_men[sex_id][
            (
                wyniki_men["analiza_id"].isin([610])
                & (wyniki_men["result"] < norm_min_density)
            )
        ].drop_duplicates()
    )

    wyniki_men["brak_ruchomych_plemnikow"] = np.where(
        wyniki_men[sex_id].isin(
            patient_id_brak_ruchomych_plemnikow), 1, 0
    )
    wyniki_men["brak_żywych_plemnikow"] = np.where(
        wyniki_men[sex_id].isin(
            patient_id_brak_żywych_plemnikow), 1, 0
    )
    wyniki_men["gestosc_plemnikow"] = np.where(
        wyniki_men[sex_id].isin(
            patient_id_gestosc_plemnikow), 1, 0
    )

    wyniki_men = wyniki_men.rename(columns={sex_id: "patient_id"})
    
    wyniki_men = wyniki_men.merge(wyniki_opis[["wizyta_id","oligospermia", "brak_plemnikow", "plemnikow_nie_stwierdzono", "teratozoospermia", "astenozoospermia", "azoospermia"]], on=["wizyta_id"], how="outer").drop_duplicates()
    wyniki_men = wyniki_men.merge(qualif_survey_results[["patient_id", "no_alive_sperm", "no_wiggling_sperm", "no_liquid_sperm", "no_sperm_in_testicle", "azoospermy"]], on="patient_id", how="outer").drop_duplicates()

    wyniki_menz = wyniki_men.groupby("patient_id").agg(
        {
            "brak_ruchomych_plemnikow": "sum",
            "brak_żywych_plemnikow": "sum",
            "gestosc_plemnikow": "sum",
            "brak_plemnikow": "sum",
            "plemnikow_nie_stwierdzono" : "sum", 
            "oligospermia" : "sum", 
            "teratozoospermia": "sum", 
            "astenozoospermia": "sum", 
            "azoospermia": "sum",
            "no_alive_sperm": "sum", 
            "no_wiggling_sperm": "sum", 
            "no_liquid_sperm": "sum", 
            "no_sperm_in_testicle": "sum", 
            "azoospermy": "sum"
        }
    )

    wyniki_menz["sum"] = wyniki_menz.sum(axis=1)

    wyniki_menz["czynnik_meski"] = np.where(wyniki_menz["sum"] > 0, 1, 0)
    wyniki_menz.reset_index(inplace=True)

    result = data.reset_index().merge(wyniki_menz[["czynnik_meski", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")

    return result

# czynnik tarczycowy


def czynnik_tarczycowy(
    data: pd.DataFrame,
    wyniki_badan: pd.DataFrame,
    wyniki_opis: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    norm_min_tsh: int = 0.4,
    norm_max_tsh: int = 4.0,
    norm_min_ft4: int = 10,
    norm_max_ft4: int = 25,
    norm_min_ft3: int = 2.25,
    norm_max_ft3: int = 6,
) -> pd.DataFrame:
    """Funkcja tworzaca tabelę z przeprocesowanymi pytaniami z ankiety pierwszorazowej oraz odpowiedziami binarnymi na czynnik tarczycowy na podstawie badań laboratoryjnych.

    Logika dla czynnika tarczycowego:
    - TSH – inne niż 0,4-4,0 mlU/l
    - FT4 - Norma: 10-25 pmol/l (8-20 ng/l)
    - FT3 - Norma: 2,25–6 pmol/l (1,5-4 ng/l)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych z odpowiedziami na przeprocesowaną ankietę pierwszorazową
    wyniki_badan : pd.DataFrame
        Wyniki badań, na podstawie których opiera się logika czynnika tarczycowego

    Returns
    -------
    pd.DataFrame
        Tabela z połączonymi odpowiedziami na pytania w ankiecie oraz czynnikiem tarczycowym
    """
    wyniki_badan = wyniki_badan.rename(columns={"id_wizyta": "wizyta_id"})

    patient_id_TSH_bad = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([83])
                & (
                    (wyniki_badan["result"] < norm_min_tsh)
                    | (wyniki_badan["result"] > norm_max_tsh)
                )
            )
        ]
    )
    patient_id_TSH_good = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([83])
                & (
                    (wyniki_badan["result"] > norm_min_tsh)
                    | (wyniki_badan["result"] < norm_max_tsh)
                )
            )
        ]
    )
    patient_id_FT4_bad = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([583])
                & (
                    (wyniki_badan["result"] < norm_min_ft4)
                    | (wyniki_badan["result"] > norm_max_ft4)
                )
            )
        ]
    )
    patient_id_FT4_good = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([583])
                & (
                    (wyniki_badan["result"] > norm_min_ft4)
                    | (wyniki_badan["result"] < norm_max_ft4)
                )
            )
        ]
    )
    patient_id_FT3_bad = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([584])
                & (
                    (wyniki_badan["result"] < norm_min_ft3)
                    | (wyniki_badan["result"] > norm_max_ft3)
                )
            )
        ]
    )

    patient_id_FT3_good = list(
        wyniki_badan.patient_id_wizyta[
            (
                wyniki_badan["analiza_id"].isin([584])
                & (
                    (wyniki_badan["result"] > norm_min_ft3)
                    | (wyniki_badan["result"] < norm_max_ft3)
                )
            )
        ]
    )

    wyniki_badan["TSH"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_TSH_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_TSH_good), 0, np.NaN)
    )
    wyniki_badan["FT4"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_FT4_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_FT4_good), 0, np.NaN)
    )
    wyniki_badan["FT3"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_FT3_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_FT3_good), 0, np.NaN)
    )
    wyniki_badan = wyniki_badan.rename(
        columns={"patient_id_wizyta": "patient_id"})
    wyniki_tarczyca = wyniki_badan.groupby("patient_id").agg(
        {"TSH": "sum", "FT4": "sum", "FT3": "sum"}
    )

    wyniki_tarczyca.reset_index(inplace=True)
    wyniki_tarczyca = wyniki_tarczyca.merge(wyniki_opis[["niedoczynnosc_tarczycy", "patient_id"]], on=[
                                            "patient_id"], how="outer").drop_duplicates()
    wyniki_tarczyca = wyniki_tarczyca.merge(qualif_survey_results[["patient_id", "overactive_thyroid", "hypothyroidism", "thyroiditis"]], on="patient_id", how="outer").drop_duplicates()

    wyniki_tarczyca["sum"] = wyniki_tarczyca[[
        "TSH", "FT4", "FT3", "niedoczynnosc_tarczycy", "overactive_thyroid", "hypothyroidism", "thyroiditis"]].sum(axis=1)

    wyniki_tarczyca["czynnik_tarczycowy"] = np.where(
        wyniki_tarczyca["sum"] > 0, 1, np.where((wyniki_tarczyca["sum"] == 0), 0, np.NaN))

    result = data.reset_index().merge(
        wyniki_tarczyca[["czynnik_tarczycowy", "patient_id"]], how="left", on="patient_id").set_index("wizyta_id")

    result = result.sort_values(
        ["wizyta_id", "czynnik_tarczycowy"], ascending=False).reset_index()
    result.drop_duplicates(subset=["wizyta_id"], keep="first", inplace=True)
    result.set_index("wizyta_id", inplace=True)

    return result


# inne czynniki endokrynologiczne

def inne_czynniki_endokrynologiczne(
    data: pd.DataFrame,
    wyniki_badan: pd.DataFrame,
    qualif_survey_results: pd.DataFrame,
    norm_min_dheas: int = 75,
    norm_max_dheas: int = 370,
    norm_min_tst: int = 0.29,
    norm_max_tst: int = 1.67,
) -> pd.DataFrame:
    """Funkcja łącząca tabelę z odpowiedziami na przeprocesowane pytania z ankiety pierwszorazowej z czynnikiem innym endokrynologicznym

    Logika dla inne czynniki endokrynologiczne:

    - Siarczan dehydroepiandrosteronu (DHEAS) norma 75-370
    - Testosteron (TST) norma 0.29-1.67

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych z przeprocesowanymi odpowiedziami na pytania z ankiety pierwszorazowej
    wyniki_badan : pd.DataFrame
        Tabela z wynikami badań laboratoryjnych

    Returns
    -------
    pd.DataFrame
        Tabela łącząca odpowiedzi na pytania z ankiety pierwszorazowej z innym czynnikiem endokrynologicznym.
    """
    wyniki_badan = wyniki_badan.rename(columns={"id_wizyta": "wizyta_id"})

    patient_id_DHEAS_bad = list(
        wyniki_badan.patient_id_wizyta[
            (wyniki_badan["analiza_id"] == 864)
            & (
                (wyniki_badan["result"] < norm_min_dheas)
                | (wyniki_badan["result"] > norm_max_dheas)
            )
        ]
    )

    patient_id_DHEAS_good = list(
        wyniki_badan.patient_id_wizyta[
            (wyniki_badan["analiza_id"] == 864)
            & (
                (wyniki_badan["result"] > norm_min_dheas)
                | (wyniki_badan["result"] < norm_max_dheas)
            )
        ]
    )
    patient_id_TST_bad = list(
        wyniki_badan.patient_id_wizyta[
            (wyniki_badan["analiza_id"] == 914)
            & (
                (wyniki_badan["result"] < norm_min_tst)
                | (wyniki_badan["result"] > norm_max_tst)
            )
        ]
    )
    patient_id_TST_good = list(
        wyniki_badan.patient_id_wizyta[
            (wyniki_badan["analiza_id"] == 914)
            & (
                (wyniki_badan["result"] > norm_min_tst)
                | (wyniki_badan["result"] < norm_max_tst)
            )
        ]
    )

    wyniki_badan["DHEAS"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_DHEAS_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_DHEAS_good), 0, np.NaN)
    )
    wyniki_badan["TST"] = np.where(
        wyniki_badan["patient_id_wizyta"].isin(patient_id_TST_bad), 1, np.where(
            wyniki_badan["patient_id_wizyta"].isin(patient_id_TST_good), 0, np.NaN)
    )
    wyniki_badan = wyniki_badan.rename(
        columns={"patient_id_wizyta": "patient_id"})
    
    wyniki_badan = wyniki_badan.merge(qualif_survey_results[["patient_id", "testorone_deficiency", "dhes_deficiency"]], on="patient_id", how="outer").drop_duplicates()


    wyniki_endo = wyniki_badan.groupby(
        "patient_id").agg({"DHEAS": "sum", "TST": "sum", "testorone_deficiency": "sum", "dhes_deficiency":"sum"})

    wyniki_endo["sum"] = wyniki_endo[["DHEAS", "TST", "testorone_deficiency", "dhes_deficiency"]].sum(axis=1)

    wyniki_endo["inne_czynniki_endokrynologiczne"] = np.where(
        wyniki_endo["sum"] > 0, 1, np.where(
            (wyniki_endo["sum"] == 0), 0, np.NaN)
    )

    result = data.reset_index().merge(
        wyniki_endo["inne_czynniki_endokrynologiczne"], how="left", on="patient_id").set_index("wizyta_id")

    return result

# wady genetyczne


def wady_genetyczne(data: pd.DataFrame, wyniki_opis: pd.DataFrame, qualif_survey_results: pd.DataFrame) -> pd.DataFrame:
    """Funkcja za pomocą, której dodawana jest kolumna z binarną oceną wady genetycznej

    Logika dla wady genetycznej:
    - stwierdzono nieprawidłowy kariotyp (kariotyp nieprawidłowy lub mozaikowy)

    Parameters
    ----------
    data : pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety

    Returns
    -------
    pd.DataFrame
        Tabela z przeprocesowanymi pytaniami z ankiety oraz z określonymi wadami genetycznymi.
    """
    kariotyp_source_one = cleaning.prepare_kariotypy_dataset()
    kariotyp_source_one = kariotyp_source_one.rename(
        columns={"patient_id_wizyta": "patient_id", "id_wizyta": "wizyta_id"})

    kariotyp = kariotyp_source_one.merge(wyniki_opis[["patient_id", "wizyta_id", "kariotyp_nieprawidłowy"]], how="outer", on=[
                                         "patient_id", "wizyta_id", "kariotyp_nieprawidłowy"]).drop_duplicates()

    kariotyp = kariotyp.merge(qualif_survey_results[["patient_id", "her_cariotype"]], on="patient_id", how="outer").drop_duplicates()

    kariotyp["wady_genetyczne"] = np.where(kariotyp["patient_id"].isin(list(kariotyp.patient_id[(kariotyp["kariotyp_nieprawidłowy"] == 1)].drop_duplicates())), 
    1,
    (
    np.where(
    kariotyp["patient_id"].isin(
        list(
            kariotyp.patient_id[
                (kariotyp["kariotyp_nieprawidłowy"] == 0)
            ].drop_duplicates())), 
    0, 
    (       
    np.where(
    kariotyp["patient_id"].isin(
        list(
            kariotyp.patient_id[
                (kariotyp["her_cariotype"] == 0)
            ].drop_duplicates()))
    , 1,
    (
    np.where(
    kariotyp["patient_id"].isin(
        list(
            kariotyp.patient_id[
                (kariotyp["her_cariotype"] == 1)
            ].drop_duplicates()))
    , 0, 
    np.NaN)))))))

    wady_genetyczne = kariotyp.groupby(
        "patient_id").agg({"wady_genetyczne": "max"})
    wady_genetyczne.reset_index(inplace=True)

    result = data.reset_index().merge(wady_genetyczne, how="left",
                                      on="patient_id").set_index("wizyta_id")

    return result

# dysfunkcje owulacyjne


def dysfunkcje_owulacyjne(data: pd.DataFrame) -> pd.DataFrame:
    """Funkcja przygotowująca tabelę z odpowiedziami binarnymi na występowanie dysfunkcji owulacyjnych i łącząca ja z odpowiedziami na pytania z ankiety pierwszorazowej
    Logika ustalona dla dyfunkcji owulacyjnych:

    - odpowiedzi na pytania w ankiecie: 
    581_Czy_kiedykolwiek_zaobserwowała_Pani_niepokojące_objawy_związane_z_układem_płciowym_(np._brak_miesiączki,_trudności_w_zapłodnieniu_itp.)?:AMENORRHOEA,
    209_429_Średnia_długość_cyklu_miesiączkowego_(liczba_dni_od_pierwszego_dnia_miesiączki_do_pierwszego_dnia_następnej_miesiączki) - powyżej 40 dni

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór odpowiedzi z przeprocesowanych ankiet pierwszorazowych

    Returns
    -------
    pd.DataFrame
        Tabela łącząca odpowiedzi z ankiet pierwszorazowych oraz kolumną z potwierdzeniem dysfunkcji owulacyjnych
    """

    data["dysfunkcje_owulacyjne"] = np.where(
        (
            data[
                'Objaw_lub_procedura:AMENORRHOEA'
            ]
            == 1
        ),
        1,
        np.where(
            (
                data[
                    '209_429_Średnia_długość_cyklu_miesiączkowego_(liczba_dni_od_pierwszego_dnia_miesiączki_do_pierwszego_dnia_następnej_miesiączki)'
                ]
                > 40
            ),
            1,
            0,
        ),
    )

    return data

# nieplodnosc idiopatyczna


def nieplodnosc_idiopatyczna(data: pd.DataFrame, wyniki_opis: pd.DataFrame, qualif_survey_results: pd.DataFrame) -> pd.DataFrame:
    """Funkcja zwracająca odpowiedzi na pytania z ankiet połączone z czynnikiem: niepłodność idiopatyczna

    Parameters
    ----------
    data : tabela z ankietami przeprocesowanymi
        pd.DataFrame

    Returns
    -------
    pd.DataFrame
        Tabela: ankieta + niepłodność idiopatyczna
    """

    #idiopat = cleaning.prepare_idiopatyczna_dataset()

    idiopat = wyniki_opis["wizyta_id", "patient_id", "idiopat"].drop_duplicates()
    qualif_survey_results = qualif_survey_results[["patient_id", "idiopathic"]].drop_duplicates()
    idiopat = idiopat.merge(qualif_survey_results[["patient_id", "idiopathic"]], on="patient_id", how="outer").drop_duplicates()

    idiopat["nieplodnosc_idiopatyczna"] = (idiopat[["idiopat", "idiopathic"]].sum(axis=1) > 0) * 1

    result = data.reset_index().merge(idiopat[["nieplodnosc_idiopatyczna", "wizyta_id", "patient_id"]], how="left", on=["wizyta_id", "patient_id"]).set_index(
        "wizyta_id"
    )

    return result

def czynnik_endometrioza(data: pd.DataFrame, wyniki_opis: pd.DataFrame, qualif_survey_results: pd.DataFrame) -> pd.DataFrame:
    """Funkcja zwracająca odpowiedzi na pytania z ankiet połączone z czynnikiem: endometrioza

    Parameters
    ----------
    data : tabela z ankietami przeprocesowanymi
        pd.DataFrame

    Returns
    -------
    pd.DataFrame
        Tabela: ankieta + endometrioza
    """

    endo = wyniki_opis.copy()
    endo["endometrioza"] = np.where(
        endo.patient_id.isin(
            list(
                endo.patient_id[(endo["endometrioza"] == 1)].drop_duplicates())
        ),
        1,
        0,
    )

    endo = endo[["patient_id", "endometrioza"]].groupby('patient_id').max()
    qualif_data = qualif_survey_results[["patient_id", "endometriosis"]].groupby('patient_id').max()

    endo = endo.merge(qualif_data, on="patient_id", how="outer")

    endo["endometrioza"] = np.where(endo[["endometriosis", "endometrioza"]].sum(axis=1) > 0, 1, 0)

    endo.reset_index(inplace=True)

    result = data.reset_index().merge(endo[["endometrioza", "patient_id"]], how="left", on=["patient_id"]).set_index(
        "wizyta_id"
    )

    return result


def add_p1_survey_logics(data: pd.DataFrame) -> pd.DataFrame:
    """Funkcja, która tworzy dodatkową logikę dla wstępnej diagnozy na podstawie odpowiedzi z ankiet pierwszorazowych

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych przeprocesowanych (odpowiedzi na pytania z ankiety pierwszorazowej + wstepna diagnoza)

    Returns
    -------
    pd.DataFrame
        Wyjściowy zbiór danych z przygotowanymi labelami
    """
    # policystyczne jajniki

    data.policystyczne_jajniki = np.where(
        (data.policystyczne_jajniki == 1)
        | (data['Objaw_lub_procedura:PCOS'] == 1),
        1,
        data.policystyczne_jajniki,
    )
    # czynnik jajowodowy jednostronny

    data.czynnik_jajowodowy_jednostronny = np.where(
        (data.czynnik_jajowodowy_jednostronny == 1)
        | (
            data[
                'Objaw_lub_procedura:DRAINAGE_FALLOPIAN_TUBES'
            ]
            == 1
        ),
        1,
        data.czynnik_jajowodowy_jednostronny,
    )
    # zaburzenia glikemii

    data.zaburzenia_glikemii = np.where(
        (data.zaburzenia_glikemii == 1)
        | (
            data[
                'Objaw_lub_procedura:DIABETES'
            ]
            == 1
        ),
        1,
        data.zaburzenia_glikemii,
    )

    # nieprawidlowosci w obrebie jajnikow

    data.nieprawidlowosci_w_obrebie_jajnikow = np.where(
        (data.nieprawidlowosci_w_obrebie_jajnikow == 1)
        | (
            data[
                '483_Czy_przeszła_Pani_zabieg_chirurgicznego_wycięcia_jednego_lub_obu_jajników?'
            ]
            == 1
        )
        | (
            data[
                'Objaw_lub_procedura:OVARIAN_CYSTS'
            ]
            == 1
        ),
        1,
        data.nieprawidlowosci_w_obrebie_jajnikow,
    )

    # czynnik tarczycowy

    data.czynnik_tarczycowy = np.where(
        (data.czynnik_tarczycowy == 1)
        | (
            data[
                'Objaw_lub_procedura:THYROID_PROBLEMS'
            ]
            == 1
        ),
        1,
        data.czynnik_tarczycowy,
    )

    # wady genetyczne

    data.wady_genetyczne = np.where(
        (data.wady_genetyczne == 1)
        | (
            data[
                '198_199_515_521_Czy_występują_w_Pani_rodzinie_jakieś_znane_Pani_choroby_genetyczne_lub_wady_wrodzone?'
            ]
            == 1
        ),
        1,
        data.wady_genetyczne,
    )

    # endometrioza

    data.endometrioza = np.where(
        (data.endometrioza == 1)
        | (
            data[
                'Objaw_lub_procedura:ENDOMETRIOSIS'
            ]
            == 1
        ),
        1,
        data.endometrioza,
    )

    return data

def input_nan(data: pd.DataFrame, male_survey: bool = False) -> pd.DataFrame:
    """Funkcja inputująca wartość zero dla diagnozy (tylko wybrane)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych przeprocesowanych + wstępna diagnoza

    Returns
    -------
    pd.DataFrame
        Zbiór danych przeprocesowanych + wstepna diagnoza (input NaN)
    """
    if male_survey == True:
        nan_to_zero = [
        "czynnik_jajowodowy_jednostronny",
        "czynnik_jajowodowy_obustronny",
        "nieprawidlowosci_w_obrebie_macicy",
        "zaburzenia_budowy_jamy_macicy",
        "nieprawidlowosci_w_obrebie_jajnikow",
        "czynnik_meski",
        "wady_genetyczne",
        # "nieplodnosc_idiopatyczna",
        "endometrioza"
    ]
    else:
        nan_to_zero = [
        "czynnik_jajowodowy_jednostronny",
        "czynnik_jajowodowy_obustronny",
        "nieprawidlowosci_w_obrebie_macicy",
        "zaburzenia_budowy_jamy_macicy",
        "nieprawidlowosci_w_obrebie_jajnikow",
        # "czynnik_meski",
        "wady_genetyczne",
        "dysfunkcje_owulacyjne",
        # "nieplodnosc_idiopatyczna",
        "endometrioza"
    ]

    for col in nan_to_zero:
        data[col].fillna(0, inplace=True)

    return data


def reorder_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Funkcja zmieniająca uporządkowanie kolumn (patient_id jako pierwsza kolumna)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych przeprocesowanych (ankieta pierwszorazowa + wstępne diagnozy)

    Returns
    -------
    pd.DataFrame
        Zbiór danych z patient_id jako pierwsza kolumna
    """
    cols_to_order = ["patient_id"]

    new_columns = cols_to_order + (data.columns.drop(cols_to_order).tolist())

    data = data[new_columns]

    return data

def male_female_results(
    data: pd.DataFrame, data_male: pd.DataFrame, test: pd.DataFrame
) -> pd.DataFrame:
    """Funkcja wykorzystująca id z ankiety p1 pacjentek (dla których wystepuje czynnik meski) i połaczenie ich z id partnera w ankietach męskich

    Args:
        data (pd.DataFrame): tabela z przeprocesowaną ankietą pacjentki
        data_male (pd.DataFrame): tabela z przeprocesowaną ankietą partnera
        compared_ids (pd.DataFrame): tabela z połączonymi id pacjentek oraz partnerów

    Returns:
        pd.DataFrame: Zwraca przeprocesowaną ankietę partnera oraz wstepną diagnozę czynnik męski
    """
    #data = data.loc[data["czynnik_meski"].notna()]
    data = data.merge(
        test[["patient_id", "patient_id_zlecenie"]], how="left", on="patient_id"
    )
    data = data.drop_duplicates()

    female_czynnik_meski_true_ids = list(
        data["patient_id_zlecenie"][data["czynnik_meski"] == 1]
    )
    female_czynnik_meski_false_ids = list(
        data["patient_id_zlecenie"][data["czynnik_meski"] == 0]
    )

    female_obnizone_true_ids = list(data["patient_id_zlecenie"][data["obnizone_parametry_nasienia"] == 1])

    female_obnizone_false_ids = list(data["patient_id_zlecenie"][data["obnizone_parametry_nasienia"] == 0])


    data_male["czynnik_meski"] = np.where(
        data_male["patient_id"].isin(female_czynnik_meski_true_ids),
        1,
        np.where(
            data_male["patient_id"].isin(female_czynnik_meski_false_ids),
            0,
            data_male["czynnik_meski"],
        ),
    )

    data_male["obnizone_parametry_nasienia"] = np.where(
    data_male["patient_id"].isin(female_obnizone_true_ids),
    1,
    np.where(
        data_male["patient_id"].isin(female_obnizone_false_ids),
        0,
        data_male["obnizone_parametry_nasienia"],
    ),
)

    return data_male


class Diagnoza:
    """Klasa łącząca odpowiedzi z ankiety pierwszorazowej ze wstępną diagnozą (on patient_id)

    load_wyniki_badan - ściąga wyniki badań
    process_factors - tworzy kolumny ze wstępną diagnozą i łączy je z ankietami pierwszorazowymi

    """

    def __init__(self, data, data_male = None) -> None:

        self.wyniki_badan: pd.DataFrame
        self.zmiany_macicy: pd.DataFrame
        self.wyniki_men: pd.DataFrame
        self.daty_ankiet: pd.DataFrame
        self.wyniki_opis: pd.DataFrame
        self.compared_ids: pd.DataFrame
        self.data_processed = data.copy()
        self.data = data
        self.data_male = data_male
        self.added_columns: List = []

    def load_wyniki_badan(
        self,
        sql: str = "wyniki_badan_kobiety",
        male_factors_only: bool = False
    ):
        """load_data pobiera dane z data warehouse. Upewnij się, że w zmiennych środowiskowych są zdefiniowane wartości USER_data_warehouse, PASSWORD_data_warehouse, DOMAIN_data_warehouse."""
        
        if male_factors_only == True:

            print("Loading data: wyniki badan mężczyzny...")

            self.wyniki_men = get_query(
                "wyniki_badan_mezczyzna",
                user="USER_data_warehouse",
                password="PASSWORD_data_warehouse",
                domain="DOMAIN_data_warehouse",
            ).drop_duplicates()

            print("Done.")
        
            self.compared_ids = get_query(
                "patient_partner_id",
                user="USER_data_warehouse",
                password="PASSWORD_data_warehouse",
                domain="DOMAIN_data_warehouse",
            ).drop_duplicates()
        
        elif male_factors_only == False:
            print("Loading data: wyniki badan zmiany macicy...")
            self.zmiany_macicy = cleaning.prepare_zmiany_macicy_dataset(
                numeric=False,
                return_cols=[
                    '[ ]zrost',
                    'miesniak',
                    'polip',
                    'jednorozn',
                    'dwurozn',
                    'przegrod',
                    'niejednorodn',
                    'ektopi',
                    'torbiel',
                ],
                opis="slownik_badanie_fizykalne_p1_cat",
                mapping_file=None,
                text_col="tresc"
            )
            self.zmiany_macicy = self.zmiany_macicy.set_index("wizyta_id")
            print("Done.")

            print("Loading data: wyniki badan kobiety...")
            self.wyniki_badan = get_query(
                sql,
                user="USER_data_warehouse",
                password="PASSWORD_data_warehouse",
                domain="DOMAIN_data_warehouse",
            )
            self.wyniki_badan = self.wyniki_badan.drop_duplicates()
            print("Done.")

        print("Loading data: data wypełnienia ankiet...")

        self.daty_ankiet = get_query(
            "visit_p1_survey_staging",
            user="USER_staging",
            password="PASSWORD_staging",
            domain="DOMAIN_staging",
        ).drop_duplicates()
        print("Done.")

        print("Loading data: wyniki z kompendium...")
        self.wyniki_opis = cleaning.prepare_all_wizyta_opis()
        print("Done")

        print("Loading: ankieta kwalifikacyjna")
        self.qualif_survey_results = process_q_survey.prep_survey()

 

    def process_factors(self, male_factors_only: bool = False, p1_survey_logics: bool = False, sex_id: str = "patient_id_wizyta"):

        if male_factors_only == False:

            initial_columns = self.data_processed.columns.tolist()
            print(f"Processing factor - czynnik jajowodowy...")
            self.data_processed = czynnik_jajowodowy(self.data_processed, self.wyniki_opis, self.qualif_survey_results)
            print(f"Done")

            print(f"Processing factor - obniżona rezerwa jajnikowa...")
            self.data_processed = obnizona_rezerwa_jajnikowa(
                self.data_processed, self.wyniki_badan, self.daty_ankiet, self.qualif_survey_results)
            print(f"Done")

            print(f"Processing factor - policystyczne jajniki...")
            self.data_processed = policystyczne_jajniki(
                self.data_processed, self.wyniki_badan, self.wyniki_opis, self.qualif_survey_results)
            print(f"Done")

            print(f"Processing factor - zaburzenia glikemii ...")
            self.data_processed = zaburzenia_glikemii(self.data_processed, self.wyniki_opis)
            print(f"Done")

            print(f"Processing factor - nieprawidlowosci w obrebie macicy...")
            self.data_processed = nieprawidlowosci_w_obrebie_macicy(
                self.data_processed, self.zmiany_macicy, self.wyniki_opis)
            print(f"Done")

            print(f"Processing factor - zaburzenia budowy jamy macicy...")
            self.data_processed = zaburzenia_budowy_jamy_macicy(
                self.data_processed, self.zmiany_macicy, self.wyniki_opis, self.qualif_survey_results)
            print(f"Done")

            print(f"Processing factor - nieprawidlowosci w obrebie jajnikow...")
            self.data_processed = nieprawidlowosci_w_obrebie_jajnikow(
                self.data_processed, self.zmiany_macicy, self.wyniki_opis)
            print(f"Done")

            print(f"Processing factor - czynnik tarczycowy...")
            self.data_processed = czynnik_tarczycowy(
                self.data_processed, self.wyniki_badan, self.wyniki_opis, self.qualif_survey_results)
            print(f"Done")

            print(f"Processing factor - inne czynniki endokrynologiczne...")
            self.data_processed = inne_czynniki_endokrynologiczne(
                self.data_processed, self.wyniki_badan, self.qualif_survey_results,)
            print(f"Done")

            print(f"Processing factor - wady genetyczne...")
            self.data_processed = wady_genetyczne(self.data_processed, self.wyniki_opis, self.qualif_survey_results)
            print(f"Done")


            # print(f"Processing factor - nieplodnosc idiopatyczna...")
            # self.data = nieplodnosc_idiopatyczna(self.data, self.wyniki_opis, self.qualif_survey_results,)
            # print(f"Done")

            print(f"Processing factor - endometrioza...")
            self.data_processed = czynnik_endometrioza(self.data_processed, self.wyniki_opis, self.qualif_survey_results,)
            print(f"Done")


            self.data_processed = reorder_columns(self.data_processed)

            if sex_id == "patient_id_wizyta":
                print(f"Processing factor - dysfunkcje owulacyjne...")
                self.data_processed = dysfunkcje_owulacyjne(self.data_processed)
                print(f"Done")

                if p1_survey_logics == True:

                    self.data_processed = add_p1_survey_logics(self.data_processed)

                    print(f"Input braków danych...")
                    self.data_processed = input_nan(self.data_processed)
                    print("Done")

            self.data_processed.drop_duplicates(inplace=True)
        
        elif male_factors_only == True:


            print(f"Processing factor - czynnik męski...")
            self.data = czynnik_meski(self.data, self.wyniki_men, self.wyniki_opis, self.qualif_survey_results, sex_id="patient_id_wizyta")
            self.data = obnizone_parametry_nasienia(self.data, self.wyniki_men, self.daty_ankiet, self.wyniki_opis, self.qualif_survey_results, sex_id="patient_id_wizyta")

            self.data_male = czynnik_meski(self.data_male, self.wyniki_men, self.wyniki_opis, self.qualif_survey_results, sex_id="patient_id_zlecenie")
            self.data_male= obnizone_parametry_nasienia(self.data_male, self.wyniki_men, self.daty_ankiet, self.wyniki_opis, self.qualif_survey_results, sex_id="patient_id_zlecenie")

            print(f"Done")

            self.data_male = male_female_results(self.data, self.data_male, self.compared_ids)
            self.data_male = self.data_male.reset_index().drop_duplicates(subset=["wizyta_id"]).set_index("wizyta_id")

            
        
