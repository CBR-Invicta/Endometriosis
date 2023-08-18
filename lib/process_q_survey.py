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
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.getenv('lib_path'))


def prep_survey(sql: str = "ankieta_kwalifikacyjna") -> pd.DataFrame:
    """Przygotowanie ankiety kwalifikacyjnej pod wstępną diagnozę

    Parameters
    ----------
    data : pd.DataFrame
        Ankieta kwalifikacyjna (sql "ankieta_kwalifikacyjna")

    Returns
    -------
    pd.DataFrame
        Przygotowana tabela z czynnikami do wstepnej diagnozy
    """

    data = get_query(
        sql,
        user="USER_replica",
        password="PASSWORD_replica",
        domain="DOMAIN_replica",
)

    qualif_survey_results = data.copy()
    qualif_survey_results = qualif_survey_results.rename(
        columns={"id_client": "patient_id"}
    )
    qualif_survey_results = qualif_survey_results.rename(
        columns={"id_meeting": "wizyta_id"}
    )

    qualif_survey_results = qualif_survey_results.sort_values(
        by=["patient_id","wizyta_id", "version"]
    )
    qualif_survey_results.drop_duplicates(
        subset=["patient_id", "wizyta_id"], keep="last", inplace=True
    )
    qualif_survey_results.drop(
        [
            "id",
            "id_process",
            "data_qualifications",
            "who",
            "when",
            "version",
            "survey_results",
        ],
        axis=1,
        inplace=True,
    )
    qualif_survey_results.set_index(["patient_id","wizyta_id"], inplace=True)
    qualif_survey_results = qualif_survey_results.survey_values.apply(pd.Series)

    qualif_survey_results = qualif_survey_results[
        qualif_survey_results.columns[
            qualif_survey_results.columns.isin(
                [
                    "ovarian_factor",
                    "pco",
                    "hormone_antymuller7",
                    "no_alive_sperm",
                    "no_wiggling_sperm",
                    "no_liquid_sperm",
                    "no_sperm_in_testicle",
                    "sperm_density_in_a_partner",
                    "azoospermy",
                    "high_sperm_fragmentation",
                    "low_hialuron",
                    "high_oxydation_stres",
                    "wrong_intercourse_test",
                    "premature_expiration_ovaries",
                    "low_ovarian_reserve",
                    "overactive_thyroid",
                    "hypothyroidism",
                    "thyroiditis",
                    "testorone_deficiency",
                    "dhes_deficiency",
                    "endometrium_disorder",
                    "uterus_loss",
                    "idiopathic",
                    "her_cariotype",
                    "his_cariotype",
                    "endometriosis",
                ]
            )
        ]
    ]

    for col in qualif_survey_results:
        qualif_survey_results[col] = qualif_survey_results[col].apply(
            pd.to_numeric, errors="coerce"
        )
        qualif_survey_results[col].replace({-1: np.NaN, 2: np.NaN}, inplace=True)
        qualif_survey_results[col].replace({0: True, 1: False}, inplace=True)
        qualif_survey_results[col].replace({True: 1, False: 0}, inplace=True)

    qualif_survey_results.reset_index(inplace=True)

    return qualif_survey_results

