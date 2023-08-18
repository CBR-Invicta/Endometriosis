from sqlalchemy import create_engine
import dotenv
import psycopg2
import os
import pandas as pd
import numpy as np
import base64
import datetime
from itertools import chain
import json
import inspect
import pickle
import dill

dotenv.load_dotenv()
sql_path = os.getenv("sql_path")


def connect_engine(
    user: str = "USER", password: str = "PASSWORD", domain: str = "DOMAIN_replica"
):
    """connect_engine allows user to connect to selected database, based on env varaibles

    Parameters
    ----------
    user : str, optional
        name of the user in ENV, by default 'USER'
    password : str, optional
        password of the user in ENV, by default "PASSWORD"
    domain : str, optional
        database you want to connect to, by default "DOMAIN_replica"

    Returns
    -------
    conn
        sql alchemy connection
    """
    try:
        user = os.getenv(user)
        password = os.getenv(password)
        domain = os.getenv(domain)
    except:
        raise ValueError(
            f"W zmiennych środowiskowych brakuje wartości {user}/{password}/{domain}."
        )

    path = "postgresql+psycopg2://" + user + ":" + password + "@" + domain
    engine = create_engine(path, execution_options={"stream_results": True})
    conn = engine.connect()
    return conn


def get_data():
    """Wczytywanie danych z kompendium (stworzonych po 01.01.2021) do sesji.

    Returns:
    -------
    df_kompleksowe : pd.DataFrame
        Tabela z wpisami z kompendium na wizytach kompleksowych. Lista dostępnych kolumn:
            nazwa_aktywnosci,
            aktywnosc_id,
            id_wizyty,
            pacjentId,
            planowanyCzasOd,
            planowanyLekarzId,
            jednOrgId,
            gabinet_id,
            pierwszyRaz,
            typ_wizyty_id,
            platnik_id,
            komentarz_do_wizyty,
            nfz_meeting_type_id
            nazwa_jednostki,
            nazwa_gabinetu,
            lokalizacja_id,
            nazwa_lokalizacji,
            nazwa_wizyty,
            provision_desc,
            requires_treatment,
            requires_treatment_desc,
            not_requires_treatment,
            not_requires_treatment_desc,
            referral_hospital,
            hints_for_referring_doctor,
            other_specialization,
            end_specialistic_treatment,
            poz_treatment
    df_other : pd.DataFrame
        Tabela z wpisami z kompendium na wizytach innych. Lista dostępnych kolumn:
            nazwa_aktywnosci,
            aktywnosc_id,
            id_wizyty,
            pacjentId,
            planowanyCzasOd,
            planowanyLekarzId,
            jednOrgId,
            gabinet_id,
            pierwszyRaz,
            typ_wizyty_id,
            platnik_id,
            komentarz_do_wizyty,
            nfz_meeting_type_id
            nazwa_jednostki,
            nazwa_gabinetu,
            lokalizacja_id,
            nazwa_lokalizacji,
            nazwa_wizyty,
            visit_type,
            hints_for_referring_doctor
    """
    conn = connect_engine()
    with open(sql_path + "/wizyty_kompleksowe.sql", "r") as query:
        query_wizyty_kompleksowe = query.read()
    with open(sql_path + "/wizyty_other.sql", "r") as query:
        query_wizyty_other = query.read()
    df_kompleksowe = pd.read_sql_query(query_wizyty_kompleksowe, conn)
    df_other = pd.read_sql_query(query_wizyty_other, conn)

    return df_kompleksowe, df_other


def get_query(name: str, **kwargs):
    """Wczytywanie danych z bazy na podstawie pliku .sql

    Parameters:
    ----------
    name : str
        Nazwa query w folderze sql, która ma posłużyć do wczytania danych z repliki

    Returns:
    -------
    data : pd.DataFrame
        Dataframe z danymi z bazy
    """
    user = kwargs.pop(
        "user",
        inspect.signature(connect_engine).parameters.get("user").default,
    )
    password = kwargs.pop(
        "password",
        inspect.signature(connect_engine).parameters.get("password").default,
    )
    domain = kwargs.pop(
        "domain",
        inspect.signature(connect_engine).parameters.get("domain").default,
    )
    with open(sql_path + "/" + name + ".sql", "r") as query:
        q = query.read()
    conn = connect_engine(user=user, password=password, domain=domain)
    data = pd.read_sql_query(q, conn)
    return data


def get_txt(file, env_variable="data_path"):
    """Wczytywanie pliku txt

    Returns:
    -------
    txt : List
        Lista z danymi z pliku
    """
    with open(
        os.getenv(env_variable) + "/" + str(file) + ".txt", "r", encoding="utf-8"
    ) as query:
        txt = query.read()
    return txt.split("\n")


def get_json(file, env_variable="data_path",folder = None):
    """Wczytywanie pliku txt

    Returns:
    -------
    txt : List
        Lista z danymi z pliku
    """
    if folder is not None:
        with open(
            os.path.join(os.getenv(env_variable),folder,str(file) + ".json"), "r", encoding="utf-8"
        ) as query:
            txt = json.load(query)
    else:
        with open(
            os.path.join(os.getenv(env_variable),str(file) + ".json"), "r", encoding="utf-8"
        ) as query:
            txt = json.load(query)
    return txt

def get_dill(file, env_variable="data_cbr"):
    """Wczytywanie pliku pickle

    Returns:
    -------
    txt : List
        Lista z danymi z pliku
    """
    with open(
        os.getenv(env_variable) + "/" + str(file) +
        ".dill", "rb"  # , encoding="utf-8"
    ) as query:
        txt = dill.load(query)
    return txt


def get_pickle(file, env_variable="data_path"):
    """Wczytywanie pliku pickle

    Returns:
    -------
    txt : List
        Lista z danymi z pliku
    """
    with open(
        os.getenv(env_variable) + "/" + str(file) +
        ".pickle", "rb"  # , encoding="utf-8"
    ) as query:
        txt = pickle.load(query)
    return txt


def get_variable_name(variable):
    globals_dict = globals()

    return [var_name for var_name in globals_dict if globals_dict[var_name] is variable]


def save_pickle(file, filename, env_variable="data_path"):
    """Zapisywanie pliku pickle
    """
    with open(
        os.getenv(env_variable) + "/" + str(filename) +
        ".pickle", "wb"  # , encoding="utf-8"
    ) as query:
        pickle.dump(file, query)
    print('Done')

def save_dill(file, filename, env_variable="data_path"):
    """Zapisywanie pliku dill
    """
    with open(
        os.getenv(env_variable) + "/" + str(filename) +
        ".dill", "wb"  # , encoding="utf-8"
    ) as query:
        dill.dump(file, query)
    print('Done')


def get_stopwords():
    """Wczytywanie stopwordsów

    Returns:
    -------
    stopwords : List
        Lista stopwordsów z pliku stopwords.txt (folder data)
    """
    with open(
        os.getenv("data_cbr") + "/" + "stopwords.txt", "r", encoding="utf-8"
    ) as query:
        stopwords = query.read()
    return stopwords.split()


def get_remove_words():
    """Wczytywanie słów, które należy usunąć z analizowanego zbioru danych kompendium

    Returns:
    -------
    remove_words : List
        Lista wyrazów do usunięcia z pliku remove_words.txt (folder data)
    """
    with open(
        os.getenv("data_cbr") + "/" + "remove_words.txt", "r", encoding="utf-8"
    ) as query:
        remove_words = query.read()
    return remove_words.split()


def get_medical_product():
    """Wczytywanie nazw produktów medycznych

    Returns:
    -------
    data : pd.DataFrame
        Tabela z nazwami plików medycznych (kolumna "name") z bazy danych.
    """
    with open(sql_path + "/medical_product.sql", "r") as query:
        q = query.read()
    conn = connect_engine()
    data = pd.read_sql_query(q, conn)
    return data


def get_prescriptions() -> pd.DataFrame:
    """get_prescriptions zwraca tabelę z przepisanymi lekami na każdą wizytę

    Returns
    -------
    pd.DataFrame
        Tabela z wynikami, gdzie 1 wiersz odpowiada jednemu przepisanemu lekowi. Aby wydobyć konkretne wartości z kolumny 'medicine', użyj .lambda(x: x[nazwa_pola_dict])
    """
    prescriptions_p1 = get_query("recepty_p1")
    prescriptions_p1["medicine"] = prescriptions_p1.prescriptions.map(
        lambda x: [y["medicine"] for y in x]
    ).to_list()
    visit_medicine = {}
    for visit_id in prescriptions_p1.meeting_id.unique():
        all_medicines = list(
            chain.from_iterable(
                prescriptions_p1.loc[
                    prescriptions_p1.meeting_id == visit_id, "medicine"
                ].tolist()
            )
        )
        visit_medicine[visit_id] = all_medicines
    prescriptions_p1_temp = pd.DataFrame.from_dict(
        visit_medicine, orient="index")
    prescriptions_p1_temp.reset_index(inplace=True, drop=False)
    prescriptions_p1_temp = prescriptions_p1_temp.melt(
        id_vars="index").dropna()
    prescriptions_p1_temp = prescriptions_p1_temp.drop(columns=["variable"]).rename(
        columns={"index": "wizyta_id", "value": "medicine"}
    )
    prescriptions_p1_temp.reset_index(inplace=True, drop=True)
    return prescriptions_p1_temp


def get_zalecenia_zlecenia(typ_zalecen: str = "short") -> pd.DataFrame:
    """get_zalecenia_zlecenia zwraca id wizyty i aktywności na wybranych P1 wraz z informacją o zaleconych działaniach z kompendium
    Parameters
    ----------
    typ_zalecen : str, optional
        typ zaleceń - short lub long, by default 'short'

    Returns
    -------
    pd.DataFrame
        Tabela z zaleceniami
    """

    df = pd.read_feather(
        os.getenv("data_cbr")
        + "/entries_bag_postep_bucket_"
        + str(typ_zalecen)
        + ".feather"
    )
    df.drop(
        columns=[
            "tresc",
            "wpisy_kompendium",
            "wpisy_kompendium_lemma",
            "wpisy_kompendium_unidecode",
        ],
        inplace=True,
    )
    return df


def get_podwyzszona_glukoza_value(visit_p1_lekarze_zlecenie=None, threshold=126):
    if visit_p1_lekarze_zlecenie is None:
        visit_p1_lekarze_zlecenie = get_query(
            "insulinoopornosc",
            user="USER_data_warehouse",
            password="PASSWORD_data_warehouse",
            domain="DOMAIN_data_warehouse",
        )
    glukoza = visit_p1_lekarze_zlecenie.loc[np.isin(
        visit_p1_lekarze_zlecenie.analiza_id, [
            651,  # GLU Glukoza w osoczu
            2531,  # Glukoza Glukoza na czczo
        ])
    ].copy()
    glukoza = glukoza.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')[['id_wizyta', 'original_result']]
    glukoza = glukoza.loc[~np.isin(glukoza.original_result, ['WYDANO', '*'])]

    glukoza.drop_duplicates(inplace=True)
    glukoza_available = glukoza.dropna().copy().set_index('id_wizyta')

    glukoza_available['podwyzszona_glukoza'] = 1 * \
        (glukoza_available.original_result.astype(float) > threshold)
    return glukoza_available


def get_homa_value(visit_p1_lekarze_zlecenie=None, threshold=2):
    if visit_p1_lekarze_zlecenie is None:
        visit_p1_lekarze_zlecenie = get_query(
            "insulinoopornosc",
            user="USER_data_warehouse",
            password="PASSWORD_data_warehouse",
            domain="DOMAIN_data_warehouse",
        )
    # analiza nazwa HOMA
    homa = visit_p1_lekarze_zlecenie.loc[visit_p1_lekarze_zlecenie.analiza_id == 2371].copy(
    )
    insulina = visit_p1_lekarze_zlecenie.loc[np.isin(
        visit_p1_lekarze_zlecenie.analiza_id, [
            841,  # INS Insulina
            2532  # INSU Insulina na czczo
        ])
    ].copy()
    glukoza = visit_p1_lekarze_zlecenie.loc[np.isin(
        visit_p1_lekarze_zlecenie.analiza_id, [
            651,  # GLU Glukoza w osoczu
            2531,  # Glukoza Glukoza na czczo
        ])
    ].copy()
    insulina = insulina.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')[['id_wizyta', 'original_result']]
    glukoza = glukoza.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')[['id_wizyta', 'original_result']]
    glukoza = glukoza.loc[~np.isin(glukoza.original_result, ['WYDANO', '*'])]
    insulina = insulina.loc[~np.isin(
        insulina.original_result, ['WYDANO', '*'])]

    results = glukoza.merge(insulina, left_on='id_wizyta',
                            right_on='id_wizyta', how='left')
    results.drop_duplicates(inplace=True)
    results_available = results.dropna().copy().set_index('id_wizyta')

    unit_mgdl_to_mmol = 0.0555
    results_available['homa'] = (results_available.original_result_y.astype(
        float) * results_available.original_result_x.astype(float) * unit_mgdl_to_mmol / 22.5)  # .set_index(results_available.id_wizyta)
    full_table = pd.DataFrame(pd.concat([homa.set_index('id_wizyta')[
                              'original_result'].rename('homa'), results_available['homa']]))
    full_table.homa = pd.to_numeric(full_table.homa)
    full_table['insulinoopornosc'] = 1*(full_table.homa > threshold)
    return full_table


import psycopg2.extras as extras
import psycopg2


def create_sql_table(df,schema:str, diagnoza:str):
    connection = psycopg2.connect(host=os.environ["db_host"], 
                              port=os.environ["db_port"], 
                              database=os.environ["db_database"], 
                              user=os.environ["db_user"], 
                              password=os.environ["db_password"]
                              )
    # Build Query Strings:
    columns_settings = [f'{x} real' for x in df.columns]
    create_table_query = f'CREATE TABLE IF NOT EXISTS {schema}.{diagnoza} ( id serial primary key, {", ".join(columns_settings)})'


    # Create Schema and Tables:
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)

def execute_values(df, schema, diagnoza):

    conn = psycopg2.connect(host=os.environ["db_host"], 
                                port=os.environ["db_port"], 
                                database=os.environ["db_database"], 
                                user=os.environ["db_user"], 
                                password=os.environ["db_password"]
                                )
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # Create table if not exits
    try:
        create_sql_table(df, schema=schema, diagnoza = diagnoza)
    except ValueError as error:
        print(error)


    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (f'{schema}.{diagnoza}', cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()