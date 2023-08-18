import spacy
from unidecode import unidecode
import string
import pandas as pd
import numpy as np
from timeit import default_timer as timer
from datetime import timedelta
from typing import List
import re
import inspect


from read_data import (
    get_stopwords,
    get_medical_product,
    get_remove_words,
    get_txt,
    get_json,
    get_query
)

import process_p1
from str_matching import match_words

stopwords = get_stopwords()
nlp = spacy.load("pl_core_news_sm")


def clean_medical_product(df, text_col="name"):
    """Czyszczenie nazw leków z bazy danych (TODO).

    Parameters:
    ----------
    df : pd.DataFrame
        Tabela z nazwami leków
    text_col : str
        Kolumna zawierająca nazwy leków

    Returns:
    -------
    df[text_col] : pd.Series
        Wyczyszczone nazwy leków
    """
    df[text_col] = (
        df[text_col]
        .str.translate(str.maketrans(" ", " ", string.punctuation))
        .str.lower()
    )
    df[text_col] = df[text_col].str.translate(
        str.maketrans("", "", string.digits))
    df[text_col] = df[text_col].replace(
        dict(zip([" " + word + " " for word in stopwords], [" "] * len(stopwords))),
        regex=True,
    )
    df[text_col] = df[text_col].replace(to_replace={"\r\n": " "}, regex=True)
    df[text_col] = df[text_col].str.strip()
    return df[text_col]


def clean_data(
    df,
    text_col="hints_for_referring_doctor",
    remove_stopwords=True,
    lematyzacja=True,
    remove_medical_product=False,
    remove_negations=False,
    paste_negations=False,
):
    """Czyszczenie danych z kompendium.

    Parameters:
    ----------
    df : pd.Dataframe
        Tabela z wpisami z kompendium
    text_col : str
        Kolumna zawierająca wpisy lekarzy
    remove_medical_product : bool
        Jeżeli True, usuwa nazwy leków w oparciu o pełną listę z bazy danych (in progress)
    remove_negations : bool
        Jeżeli true, usuwa całość negacji (preferowane w analizie wektorowej). W innym przypadku łączy przedrostek 'nie' z sąsiadującym wyrazem.

    Returns:
    -------
    df : pd.DataFrame
        Pełna tabela z dodatkową kolumną 'wpisy_kompendium'
    df_notna : pd.DataFrame
        Tabela z niepustymi wpisami, po lematyzacji i unidecode
    """
    stopwords = get_stopwords()
    remove_words = get_remove_words()
    print("Usuwanie interpunkcji i ujednolicenie rozmiaru liter...")
    start = timer()
    df["wpisy_kompendium"] = (
        df[text_col]
        .str.translate(str.maketrans(" ", " ", string.punctuation))
        .str.lower()
    )
    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    print("Usuwanie cyfr...")  # Usuwanie cyfr - jeżeli są niezależnie
    start = timer()

    df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
        to_replace={"(?<=\s)(\d+)(?=\s)": " "}, regex=True
    )

    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    # normalizacja "nie" - łączymy z kolejnym słowem
    start = timer()
    if remove_negations is True:
        print("Usuwanie znaczników nowych linii oraz negacji...")

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            to_replace={
                "\r\n": " ",
                "\n\r": " ",
                "\n": " ",
                "\r": " ",
                "\t": " ",
                "  *": " ",
                "\d+x": "",
                " nie \r\n": " ",
                " nie\r\n": " ",
                " nie jest ": " ",
                " nie sa ": " ",
                " nie są ": " ",
                " nie ": " ",
            },
            regex=True,
        )
    elif paste_negations is True:
        print("Usuwanie znaczników nowych linii, łączenie przedrostka nie...")

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            to_replace={
                "\r\n": " ",
                "\n\r": " ",
                "\n \r": " ",
                "  *": " ",
                "\d+x": "",
                " nie \r\n": "-nie ",
                " nie\r\n": "-nie ",
                " nie jest ": " nie",
                " nie sa ": " nie",
                " nie są ": " nie",
                " nie ": " nie",
            },
            regex=True,
        )
    else:
        print("Usuwanie znaczników nowych linii...")

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            to_replace={
                "\r\n": " ",
                "\n\r": " ",
                "\n \r": " ",
                "  *": " ",
                "\d+x": "",
            },
            regex=True,
        )
    df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
        to_replace={"anty ": "anty"}, regex=True
    )
    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    if remove_stopwords is True:
        print("Usuwanie stopwordsów...")
        start = timer()

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            dict(
                zip([r"\b" + word + r"\b" for word in stopwords],
                    [""] * len(stopwords))
            ),
            regex=True,
        )

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            dict(
                zip(
                    [r"\b" + word + r"\b" for word in remove_words],
                    [""] * len(remove_words),
                )
            ),
            regex=True,
        )

        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            r"[\x20]{2,}",
            " ",
            regex=True,
        )
        end = timer()
        print(f"Done in {timedelta(seconds=end-start)}")

    # Opcjonalnie - usuwanie nazw leków (pełnych)
    if remove_medical_product is True:
        print()
        medical_product = clean_medical_product(
            get_medical_product()).to_list()
        df["wpisy_kompendium"] = df["wpisy_kompendium"].replace(
            dict(
                zip(
                    [" " + word + " " for word in medical_product],
                    [" "] * len(medical_product),
                )
            ),
            regex=True,
        )
    print("Usuwanie pustych wpisów...")
    start = timer()

    df["wpisy_kompendium"] = df["wpisy_kompendium"].apply(
        lambda x: None if len(x) == 0 else x
    )
    df_notna = df.dropna(subset=["wpisy_kompendium"]).copy()

    print(f"Usunięto {df.shape[0]-df_notna.shape[0]} wierszy.")
    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    print("Listowanie wyrazów...")  # (spacja jako przerywnik)
    start = timer()

    df_notna["wpisy_kompendium"] = df_notna["wpisy_kompendium"].str.strip()

    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    if lematyzacja is True:
        print("Lematyzacja...")  # przy pomocy spacy
        start = timer()

        df_notna["wpisy_kompendium_lemma"] = df_notna["wpisy_kompendium"].apply(
            lambda x: [i.lemma_ for i in nlp(x)]
        )

        end = timer()
        print(f"Done in {timedelta(seconds=end-start)}")
        print("Usuwanie polskich znaków...")
        start = timer()

        df_notna["wpisy_kompendium_unidecode"] = df_notna[
            "wpisy_kompendium_lemma"
        ].apply(lambda x: [unidecode(token) for token in x])

        df_notna["wpisy_kompendium_unidecode"] = df_notna[
            "wpisy_kompendium_unidecode"
        ].apply(lambda x: [token for token in x if token not in remove_words])

        end = timer()
        print(f"Done in {timedelta(seconds=end-start)}")
    df_notna.reset_index(inplace=True, drop=True)
    return df, df_notna


def unidecode_list(lista: List[str]) -> List[str]:
    """unidecode_list pozwala na unidecode wyrazów w liście

    Parameters
    ----------
    lista : List[str]
        lista slów wejściowych

    Returns
    -------
    List[str]
        lista po unidecode
    """
    print("Usuwanie polskich znaków...")
    start = timer()
    lista_unidecode = [unidecode(word).lower() for word in lista]
    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    return lista_unidecode


def lemma_list(lista: List[str]) -> List[str]:
    """lemma_list pozwala na lematyzację wyrazów w liście

    Parameters
    ----------
    lista : List[str]
        lista slów wejściowych

    Returns
    -------
    List[str]
        lista po lematyzacji
    """
    print("Lematyzacja...")  # przy pomocy spacy
    start = timer()
    lista_lemma = [" ".join([i.lemma_ for i in nlp(word)]) for word in lista]
    end = timer()
    print(f"Done in {timedelta(seconds=end-start)}")
    return lista_lemma


def bagging_wpisy(
    entries: pd.DataFrame, keyphrases_df: pd.DataFrame, entries_bag: pd.DataFrame
) -> pd.DataFrame:
    for entry_id, entry in enumerate(entries):
        if not entry_id % 100:
            print(entry_id)
        entry_bag = np.zeros(len(keyphrases_df), dtype=np.uint8)
        # print("============================================================")
        # print(" ".join(entry))
        for entry_word_id, _ in enumerate(entry):
            for kp_id, key_phrase in enumerate(
                keyphrases_df.wpisy_kompendium_unidecode.to_list()
            ):
                if entry_word_id > len(entry) - len(key_phrase):
                    continue
                for kw_id, keyword in enumerate(key_phrase):
                    try:
                        result, _ = match_words(
                            entry[entry_word_id + kw_id], keyword, 0.8
                        )
                    except ZeroDivisionError:
                        result = False
                    if result:
                        if kw_id >= len(key_phrase) - 1:  # wyrazy w kp się skończyły
                            # print(similarity, "|", " ".join(entry[entry_word_id:entry_word_id+kw_id+1]) ,"|",  " ".join(key_phrase))
                            entry_bag[kp_id] += 1
                        elif (
                            entry_word_id + kw_id < len(entry) - 1
                        ):  # wyrazy w entry się nie skończyły i starcz
                            continue
                        else:
                            # print("za krotki")
                            pass
                    else:
                        break
        entries_bag.loc[entry_id] = entry_bag
    return entries_bag


def find_characteristics(
    df: pd.DataFrame, opis: List[str], text_col: str = "wpisy_kompendium"
) -> dict:
    """find_characteristics przeszukuje wpisy w kompendium pod kątem charakterystycznych opisów

    Parameters
    ----------
    df : pd.DataFrame
        _description_
        pd.Series zawierająca wpisy w kompendium
    opis : List[str]
        lista opisów z pliku źródłowego
    text_col : str, optional
        _description_, by default 'wpisy_kompendium'

    Returns
    -------
    dict
        słownik z opisem macic oraz listą wystąpień we wpisach w kompendium
    """

    wpisy = unidecode_list(df[text_col].to_list())
    opis = unidecode_list(opis)
    opis_prepared = [word[:-1] + "." if word[-1]
                    == "a" else word for word in opis]
    mapping_opis = dict(zip(opis_prepared, opis))
    appears = {}
    for word in opis_prepared:
        appears[mapping_opis[word]] = [
            re.search(word, sentence) is not None for sentence in wpisy
        ]
    return appears


def add_badanie_fizykalne_p1_characteristic(
    df: pd.DataFrame, opis: str, mapping_file: str = None, **kwargs
) -> pd.DataFrame:
    """add_badanie_fizykalne_p1_characteristic dodaje cechy binarne do wpisów w kompendium z pola KLN Badanie Fizykalne na wizytach P1

    Parameters
    ----------
    df : pd.DataFrame
        Tabela z wpisami w kompendium
    opis : str
        Nazwa pliku z opisami do wyciągnięcia z kompendium (domyślnie z folderu DATA_cbr)

    Returns
    -------
    pd.DataFrame
        Tabela z dopisanymi zmiennymi
    """
    df.reset_index(inplace=True, drop=True)
    # Get variables from optional **kwargs
    env_variable = kwargs.pop(
        "env_variable",
        inspect.signature(get_txt).parameters.get("env_variable").default,
    )
    text_col = kwargs.pop(
        "text_col",
        inspect.signature(find_characteristics).parameters.get(
            "text_col").default,
    )
    # Get files data
    opis_data = get_txt(
        file=opis,
        env_variable=env_variable,
    )
    if mapping_file is not None:
        mapping_file_data = get_json(
            file=mapping_file,
            env_variable=env_variable,
        )
    # Find rows with characteristics
    appeared = find_characteristics(df=df, opis=opis_data, text_col=text_col)
    # Merge with original data
    appeared = pd.DataFrame.from_dict(appeared)
    if mapping_file is not None:
        new_cols = {}
        for key, value in mapping_file_data.items():
            new_cols[key] = appeared[value].sum(axis=1)
        appeared = pd.DataFrame.from_dict(new_cols)
    appeared = (appeared > 0).astype(int)
    df[appeared.columns.to_list()] = appeared

    return df


def find_numeric(
    df: pd.DataFrame, opis: List[str], text_col: str = "wpisy_kompendium"
) -> dict:
    """find_numeric przeszukuje wpisy w kompendium pod kątem istotnych wartości liczbowych

    (Upewnij się, że w regex szukana wartość jest w ostatniej grupie)

    Parameters
    ----------
    df : pd.DataFrame
        _description_
        pd.Series zawierająca wpisy w kompendium
    opis : List[str]
        lista opisów z pliku źródłowego
    text_col : str, optional
        _description_, by default 'wpisy_kompendium'

    Returns
    -------
    dict
        Słownik z szukanymi wartościami oraz wartością liczbową
    """

    wpisy = unidecode_list(df[text_col].to_list())
    appears = {}
    for word in opis:
        appears[word] = [
            re.search(word, sentence).group(
                len(re.search(word, sentence).groups()))
            if re.search(word, sentence) is not None
            else np.nan
            for sentence in wpisy
        ]
    return appears


def add_badanie_fizykalne_p1_numeric(
    df: pd.DataFrame, opis: str, mapping_file: str = None, **kwargs
) -> pd.DataFrame:
    """add_badanie_fizykalne_p1_characteristic dodaje cechy binarne do wpisów w kompendium z pola KLN Badanie Fizykalne na wizytach P1

    Parameters
    ----------
    df : pd.DataFrame
        Tabela z wpisami w kompendium
    opis : str
        Nazwa pliku z opisami do wyciągnięcia z kompendium (domyślnie z folderu DATA_cbr)

    Returns
    -------
    pd.DataFrame
        Tabela z dopisanymi zmiennymi
    """
    df.reset_index(inplace=True, drop=True)
    # Get variables from optional **kwargs
    env_variable = kwargs.pop(
        "env_variable",
        inspect.signature(get_txt).parameters.get("env_variable").default,
    )
    text_col = kwargs.pop(
        "text_col",
        inspect.signature(find_characteristics).parameters.get(
            "text_col").default,
    )
    df[text_col] = df[text_col].str.lower()
    # Get files data
    opis_data = get_txt(
        file=opis,
        env_variable=env_variable,
    )
    if mapping_file is not None:
        mapping_file_data = get_json(
            file=mapping_file,
            env_variable=env_variable,
        )
    # Find rows with characteristics
    appeared = find_numeric(df=df, opis=opis_data, text_col=text_col)
    # Merge with original data
    appeared = pd.DataFrame.from_dict(appeared)
    for column_name in appeared:
        appeared[column_name] = (
            appeared[column_name]
            .replace(to_replace={",+": ".", "\.+": "."}, regex=True)
            .astype(float)
        )
    if mapping_file is not None:
        new_cols = {}
        for key, value in mapping_file_data.items():
            new_cols[key] = appeared[value].max(axis=1, skipna=True)
        appeared = pd.DataFrame.from_dict(new_cols)
    df[appeared.columns.to_list()] = appeared

    return df


def get_data_from_fizykalne(
        source='usg_dicom',
        numeric=True,
        return_cols=["afc_jp", "afc_jl"],
        opis="slownik_badanie_fizykalne_p1_num",
        mapping_file="mapowanie_badanie_fizykalne_p1_num",
        text_col="tresc"):
    badanie_fizykalne = get_query(source,user="USER_replica",
    password="PASSWORD_replica",
    domain="DOMAIN_replica")
    if numeric:
        badania_fizykalne_data = add_badanie_fizykalne_p1_numeric(
            df=badanie_fizykalne,
            opis=opis,
            mapping_file=mapping_file,
            text_col=text_col,
            env_variable="DATA_cbr",
        )
    else:
        badania_fizykalne_data = add_badanie_fizykalne_p1_characteristic(
            df=badanie_fizykalne,
            opis=opis,
            mapping_file=mapping_file,
            text_col=text_col,
            env_variable="DATA_cbr",
        )
    return_data = badania_fizykalne_data.dropna(
        subset=return_cols, how='all'
    )
    return_cols = ["patient_id", "wizyta_id", 'result_time'] + return_cols
    return return_data[return_cols].drop_duplicates()


def prepare_afc_dataset(sources=['usg_dicom', 'badanie_fizykalne']) -> pd.DataFrame:
    dfs = []
    for source in sources:
        dfs.append(get_data_from_fizykalne(source=source))
    return pd.concat(dfs).drop_duplicates()


def prepare_zmiany_macicy_dataset(sources=['usg_dicom', 'badanie_fizykalne'], **kwargs) -> pd.DataFrame:
    dfs = []
    for source in sources:
        dfs.append(get_data_from_fizykalne(source=source, **kwargs))
    return pd.concat(dfs).drop_duplicates().rename(mapper={'[ ]zrost': 'zrosty'}, axis=1).groupby(['wizyta_id', 'result_time']).sum().reset_index()


def prepare_zalecenie_redukcji_masy_ciala_dataset(threshold=25):
    redukcja_masy_ciala = get_query('redukcja_masy_ciala',
                                    user="USER_staging",
                                    password="PASSWORD_staging",
                                    domain="DOMAIN_staging")
    data = process_p1.process_question_92(redukcja_masy_ciala).merge(
        process_p1.process_numeric_questions(
            redukcja_masy_ciala, question_id=90),
        left_index=True,
        right_index=True,
        how='outer'
    )
    data = data.loc[~data.index.duplicated(keep='first')]
    response = process_p1.process_calculate_bmi(data)
    zalecenia = (response > threshold)
    return zalecenia.rename(columns={'90_92_BMI': 'zalecenie_redukcji_masy_ciala'}).reset_index()


def prepare_ushsg_dataset() -> pd.DataFrame:
    """prepare_ushsg_dataset wyciąga dane ze źródła USGHS

    Returns
    -------
    pd.DataFrame
        Funkcja zwraca tabelę z dodatkowymi kolumnami: 
         - 'gromadzenie_plynu' świadczy o nieprawidłowej drożności
         - 'opor_przy_podawaniu_plynu' świadczy o nieprawidłowej drożności
         - 'czynnik_jajowodowy_jednostronny' świadczy o nieprawidłowej drożności
         - 'czynnik_jajowodowy_obustronny' świadczy o nieprawidłowej drożności
         - 'zaburzenia_budowy_jamy_macicy' odpowadają za inny typ diagnozy (inne zaburzenia budowy jamy macicy)
         - 'norma' wskazuje na drożność jajowodów, swobodny spływ płynu raz zwykły kształt macicy (0 - żaden czynnik nie jest spełniony, 4 - wszystkie czynniki są spełnione)
    """
    ushsg_dataset = get_query('czynnik_jajowodowy', user="USER_replica",
    password="PASSWORD_replica",
    domain="DOMAIN_replica")
    ushsg_dataset_unique = ushsg_dataset.drop_duplicates(
        subset='wizyta_id', keep='last').copy()
    ushsg_dataset_unique_not_empty = ushsg_dataset_unique.loc[
        (ushsg_dataset_unique.drop(columns="wizyta_id").sum(axis=1) > 0)
    ]
    ushsg_dataset_unique_not_empty[
        "gromadzenie_plynu"
    ] = ushsg_dataset_unique_not_empty[[
        "plyn_gromadzi_sie_w_poblizu_prawego_rogu_macicy",
        "plyn_gromadzi_sie_w_poblizu_lewego_rogu_macicy"
    ]].sum(axis=1)

    ushsg_dataset_unique_not_empty[
        "opor_przy_podawaniu_plynu"
    ] = ushsg_dataset_unique_not_empty[[
        "silny_opor_przy_podawaniu_plynu",
        "zwiekszony_opor_przy_podawaniu_plynu"
    ]].sum(axis=1)

    ushsg_dataset_unique_not_empty[
        "zaburzenia_budowy_jamy_macicy"
    ] = ushsg_dataset_unique_not_empty[[
        "zwezenie_kanalu_szyjki_macicy",
        "nieprawidlowe_echo_w_jamie_macicy",
    ]].sum(axis=1)

    ushsg_dataset_unique_not_empty[
        "czynnik_jajowodowy_jednostronny"
    ] = ushsg_dataset_unique_not_empty[[
        'podejrzenie_niedroznosci_jajowodu_prawego',
        'podejrzenie_niedroznosci_jajowodu_lewego'
    ]].max(axis=1)*1

    ushsg_dataset_unique_not_empty[
        "czynnik_jajowodowy_obustronny"
    ] = (ushsg_dataset_unique_not_empty[[
        'podejrzenie_niedroznosci_jajowodu_prawego',
        'podejrzenie_niedroznosci_jajowodu_lewego'
    ]].sum(axis=1) == 2)*1

    ushsg_dataset_unique_not_empty[
        "norma"
    ] = ushsg_dataset_unique_not_empty[[
        'jama_macicy_zwyklego_ksztaltu_i_pojemnosci',
        'jajowod_prawy_drozny',
        'jajowod_lewy_drozny',
        'plyn_swobodnie_splywa_do_zatoki_douglasa'
    ]].sum(axis=1)
    return ushsg_dataset_unique_not_empty[['patient_id','wizyta_id', 'result_time', 'gromadzenie_plynu', 'opor_przy_podawaniu_plynu', 'zaburzenia_budowy_jamy_macicy', 'czynnik_jajowodowy_jednostronny', 'czynnik_jajowodowy_obustronny', 'norma']]


def prepare_kariotypy_dataset():
    """prepare_kariotypy_dataset tworzy tabelę z wynikami kariotypu.

    Wartości w kolumnie result:
     1 - kariotyp nie był prawidłowy
     2 - kariotyp prawidłowy

    Returns
    -------
    pd.DataFrame
        Tabela z resultatami
    """
    kariotypy = get_query(
        "kariotypy",
        user="USER_data_warehouse",
        password="PASSWORD_data_warehouse",
        domain="DOMAIN_data_warehouse",
    ).drop_duplicates()
    kariotypy['original_result'] = kariotypy.original_result.str.extract(
        '(mozaikowy|prawidłowy|nieprawidłowy)', expand=False)
    kariotypy['kariotyp_nieprawidłowy'] = np.where(kariotypy.original_result.isin(['nieprawidłowy', 'mozaikowy']), 1, np.where(kariotypy.original_result.isin(['prawidłowy']), 0, np.NaN))
    # pd.get_dummies(data=kariotypy, prefix='kariotyp', columns=['original_result'])[['id_wizyta', 'result_time', 'partner_zlecenie', 'kariotyp_nieprawidłowy', 'kariotyp_prawidłowy']]

    return kariotypy[['patient_id_wizyta', 'id_wizyta', 'kariotyp_nieprawidłowy', 'result_time', 'partner_zlecenie']]


def prepare_nawracajace_poronienia_dataset():
    nawracajace_poronienia = get_query('nawracajace_poronienia',
                                       user="USER_staging",
                                       password="PASSWORD_staging",
                                       domain="DOMAIN_staging")
    return nawracajace_poronienia.rename(mapper={'odpowiedz': 'nawracajace_poronienia'}, axis=1)[['wizyta_id', 'result_time', 'nawracajace_poronienia']]


def prepare_podwyzszona_glukoza_dataset(threshold=126):
    glukoza = get_query(
        "glukoza",
        user="USER_data_warehouse",
        password="PASSWORD_data_warehouse",
        domain="DOMAIN_data_warehouse",
    ).drop_duplicates()
    glukoza = glukoza.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')
    # glukoza = glukoza.loc[~np.isin(glukoza.result, ['WYDANO', '*'])]

    glukoza.drop_duplicates(inplace=True)
    glukoza_available = glukoza.dropna().copy()  # .set_index('id_wizyta')

    glukoza_available['podwyzszona_glukoza'] = 1 * \
        (glukoza_available.result.astype(float) > threshold)
    return glukoza_available


def prepare_homa_dataset(threshold=2):
    source_data = get_query(
        "homa",
        user="USER_data_warehouse",
        password="PASSWORD_data_warehouse",
        domain="DOMAIN_data_warehouse",
    ).drop_duplicates()
    homa = source_data.loc[source_data.analiza_id == 2371].copy(
    )
    insulina = source_data.loc[np.isin(
        source_data.analiza_id, [
            841,  # INS Insulina
            2532  # INSU Insulina na czczo
        ])
    ].copy()
    glukoza = source_data.loc[np.isin(
        source_data.analiza_id, [
            651,  # GLU Glukoza w osoczu
            2531,  # Glukoza Glukoza na czczo
        ])
    ].copy()
    insulina = insulina.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')
    glukoza = glukoza.sort_values(['id_wizyta', 'result_time']).drop_duplicates(
        subset=['id_wizyta'], keep='first')

    results = glukoza.merge(insulina, left_on='id_wizyta',
                            right_on='id_wizyta', how='left')
    results.drop_duplicates(inplace=True)
    results_available = results.dropna().copy().set_index('id_wizyta')

    unit_mgdl_to_mmol = 0.0555
    results_available['result'] = (results_available.result_y.astype(
        float) * results_available.result_x.astype(float) * unit_mgdl_to_mmol / 22.5)  # .set_index(results_available.id_wizyta)
    full_table = pd.DataFrame(pd.concat([homa.set_index('id_wizyta')[
                              'result'], results_available['result']]))
    full_table['result'] = pd.to_numeric(full_table.result)
    full_table['insulinoopornosc'] = 1*(full_table.result > threshold)
    full_table = full_table.merge(
        source_data[['id_wizyta', 'result_time', 'partner_zlecenie']].sort_values(['id_wizyta', 'result_time']).drop_duplicates(
            subset=['id_wizyta'], keep='first'), left_index=True, right_on='id_wizyta')
    return full_table[['id_wizyta', 'result', 'partner_zlecenie', 'result_time',
                       'insulinoopornosc']]  # .reset_index()


def prepare_test_hialuronowy_dataset():
    test_hialuronowy = get_query(
        "test_hialuronowy",
        user="USER_data_warehouse",
        password="PASSWORD_data_warehouse",
        domain="DOMAIN_data_warehouse",
    ).drop_duplicates()
    return test_hialuronowy.query('result<888888')

# funkcje dotyczace wyciąganie informacji ze żródła wizyta-opis dla formularza id  66000 - wizyta kwalifikacyjna

def add_badanie_fizykalne_p1_binary(
    df: pd.DataFrame, opis: str, mapping_file: str = None, **kwargs
) -> pd.DataFrame:
    """add_badanie_fizykalne_p1_characteristic dodaje cechy binarne do wpisów podczas wizyt kwalifikacyjnych

    Parameters
    ----------
    df : pd.DataFrame
        Tabela z wpisami w kompendium
    opis : str
        Nazwa pliku z opisami do wyciągnięcia z kompendium (domyślnie z folderu DATA_cbr)

    Returns
    -------
    pd.DataFrame
        Tabela z dopisanymi zmiennymi
    """
    df.reset_index(inplace=True, drop=True)
    # Get variables from optional **kwargs
    env_variable = kwargs.pop(
        "env_variable",
        inspect.signature(get_txt).parameters.get("env_variable").default,
    )
    text_col = kwargs.pop(
        "text_col",
        inspect.signature(find_characteristics).parameters.get(
            "text_col").default,
    )
    df[text_col] = df[text_col].str.lower()
    # Get files data
    opis_data = get_txt(
        file=opis,
        env_variable=env_variable,
    )
    if mapping_file is not None:
        mapping_file_data = get_json(
            file=mapping_file,
            env_variable=env_variable,
        )
    # Find rows with characteristics
    appeared = find_numeric(df=df, opis=opis_data, text_col=text_col)
    # Merge with original data
    appeared = pd.DataFrame.from_dict(appeared)
    for column_name in appeared:
        appeared[column_name] = (
            appeared[column_name]
            .replace(to_replace={",+": ".", "\.+": "."}, regex=True)
            #.astype(float)
        )
    if mapping_file is not None:
        new_cols = {}
        for key, value in mapping_file_data.items():
            new_cols[key] = appeared[value].max(axis=1, skipna=True)
        appeared = pd.DataFrame.from_dict(new_cols)
    df[appeared.columns.to_list()] = appeared

    return df

def get_data_from_wizyta_opis(
        source='wizyta_opis',
        numeric=False,
        binary=False,
        return_cols=["[ ]rif[^ua]"],
        opis="slownik_badanie_fizykalne_p1_cat",
        mapping_file="mapowanie_badanie_fizykalne_p1_num",
        text_col="tresc"):
    """Funkcja zwracająca poszukiwaną informację w kolumnach w formie 0-1.

    Parameters
    ----------
    source : str, optional
        źródło, by default 'wizyta_opis'
    numeric : bool, optional
        gdy szukana informacja jest wartością numeryczną, by default False
    binary : bool, optional
        gdy szukana informacja jest wartoscia binarna, by default False
    return_cols : list, optional
        szukane wyrazenie regex, by default ["[ ]rif[^ua]"]
    opis : str, optional
        regex w notatniku, by default "slownik_badanie_fizykalne_p1_cat"
    mapping_file : str, optional
        mapowanie regexow, by default "mapowanie_badanie_fizykalne_p1_num"
    text_col : str, optional
        z jakiej kolumny źródła wyciągnąć informacje, by default "tresc"

    Returns
    -------
    pd.DataFrame
        Funkcja zwraca tabelę z wizytą id oraz z szukanym regexem
    """
    wizyta_opis = get_query(name=source,user="USER_replica",
    password="PASSWORD_replica",
    domain="DOMAIN_replica")
    if numeric:
        wizyta_opis_data = add_badanie_fizykalne_p1_numeric(
            df=wizyta_opis,
            opis=opis,
            mapping_file=mapping_file,
            text_col=text_col,
            env_variable="DATA_cbr",
        )
    elif binary:
        wizyta_opis_data = add_badanie_fizykalne_p1_binary(
            df=wizyta_opis,
            opis=opis,
            mapping_file=mapping_file,
            text_col=text_col,
            env_variable="DATA_cbr",
        )        
    else:
        wizyta_opis_data = add_badanie_fizykalne_p1_characteristic(
            df=wizyta_opis,
            opis=opis,
            #mapping_file=mapping_file,
            text_col=text_col,
            env_variable="DATA_cbr",
        )
    return_data = wizyta_opis_data.dropna(
        subset=return_cols, how='all'
    )
    return_cols = ["patient_id", "wizyta_id"] + return_cols
    return return_data[return_cols].drop_duplicates()

# Funkcje dla poszczególnych regexów - źródło wizyta_opis

def prepare_rif_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja zwracająca tabelę, gdzie kolumny to wizyta id oraz nawaracająca niewydolność implantacyjna RIF

    Parameters
    ----------
    sources : list, optional
        źródło, by default ["wizyta_opis"]

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, RIF
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis())
    return pd.concat(dfs).drop_duplicates().rename(mapper={'[ ]rif[^ua]': 'rif'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_menarche_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca tabelę, gdzie kolumny to wizyta id oraz informacja o menarche

    Parameters
    ----------
    sources : list, optional
        źródło, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, menarche
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, numeric=True, return_cols=["menarche"], opis="slownik_badanie_fizykalne_p1_num"))
    return pd.concat(dfs).drop_duplicates()

def prepare_nieprawidlowosci_jajniki_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja wyciągająca ze źródła wyniki_opis informacje na temat wystepowania torbieli jajnika

    Parameters
    ----------
    sources : list, optional
        źrodło, by default ["wizyta_opis"]

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta_id, torbiel
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(return_cols=["torbiel.jajnik"]))
    return pd.concat(dfs).drop_duplicates().rename(mapper={'torbiel.jajnik': 'torbiel'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_papierosy_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """funkcja zwracająca informację o paleniu papierosów przez pacjentkę

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, papierosy (1 tak 0 nie)
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, binary=True, return_cols=["papierosy"], opis="slownik_badanie_fizykalne_p1_bin", mapping_file="mapowanie_badanie_fizykalne_p1_bin"))
        dfs = pd.concat(dfs).drop_duplicates()
        dfs["papierosy"] = dfs["papierosy"].map({'tak':1, 'nie': 0})
    return dfs

def prepare_bmi_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca tabelę z trzema kolumnami: waga, wzrost oaz na ich podstawie obliczone BMI

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, wzrost, waga, BMI
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, numeric=True, return_cols=["wzrost", "waga"], opis="slownik_badanie_fizykalne_p1_num"))
    dfs = pd.concat(dfs).drop_duplicates()
    dfs["wzrost"] = np.where(dfs["wzrost"] > 250, np.NaN, np.where(dfs["wzrost"] < 100, np.NaN, dfs["wzrost"]/ 100))
    dfs["waga"] = np.where(dfs["waga"] > 250, np.NaN, np.where(dfs["waga"] < 35, np.NaN, dfs["waga"]))
    dfs["BMI"] = np.round((np.where((dfs["wzrost"].notna() & dfs["wzrost"].notna()), dfs["waga"]/dfs["wzrost"]**2, np.NaN)), 2)
    #dfs["BMI"] = dfs["waga"]/dfs["waga"]*2

    return dfs

def prepare_endometrioza_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca tabelę z informacją o występowaniu endometriozy

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, endometrioza
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["endometrioz.(?![-nie| - nie|?])"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={'endometrioz.(?![-nie| - nie|?])': 'endometrioza'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_jajnikowy_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca informacje o występowaniu czynnika jajnikowego na podstawie opisu z wizyt kwalifikacyjnych

    Parameters
    ----------
    sources : list, optional
        źródło, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, czynnik jajnikowy
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["czynnik jajnikowy.(?!nie)"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={'czynnik jajnikowy.(?!nie)': 'czynnik_jajnikowy'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_niedoczynnosc_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca informacje o niedoczynnosci pacjentki

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, niedocyznnosc tarczycy
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["nie.*tarczycy"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={'nie.*tarczycy': 'niedoczynnosc_tarczycy'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_rezerwajajnikowa_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca wyniki AMH z opsów wpisów i kolumnę z obniżoną rezerwą jajnikową

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, AMH, obniżona rezerwa jajnikowa
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, numeric=True, return_cols=["AMH"], opis="slownik_badanie_fizykalne_p1_num"))
    dfs = pd.concat(dfs).drop_duplicates()

    dfs["AMH"] = np.where(dfs["AMH"] > 20, np.NaN, dfs["AMH"])
    dfs["obnizona_rezerwa"] = np.where(dfs["AMH"] < 1, 1, np.where(dfs["AMH"].isna(), np.NaN, 0))

    return dfs

def prepare_pcos_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca informacje o wystepowaniu zespołu PCOs u pacjentki

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, PCOs
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["pcos(?![ podejrz*])"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))
        dfs.append(get_data_from_wizyta_opis(source=source, numeric=True, return_cols=["AMH"], opis="slownik_badanie_fizykalne_p1_num"))
    
    dfs = pd.concat(dfs).drop_duplicates().rename(mapper={'pcos(?![ podejrz*])': 'policystyczne_jajniki'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

    dfs["AMH"] = np.where(dfs["AMH"] > 20, np.NaN, dfs["AMH"])
    dfs["policystyczne_jajniki"] = np.where(dfs["AMH"] > 6, 1, dfs["policystyczne_jajniki"])

    return dfs

def prepare_AFC_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca wyniki AFC dla pacentki na podstawie źródła wyniki opis dla wizyt kwalifikacyjnych

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, wyniki AFC
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, numeric=True, return_cols=["AFC"], opis="slownik_badanie_fizykalne_p1_num"))

    dfs = pd.concat(dfs).drop_duplicates()
    dfs["AFC"] = np.where(dfs["AFC"] > 50, np.NaN, dfs["AFC"])
    
    return dfs

def prepare_jajowodowy_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca informaacje o wystepowaniu czynnika jajowedowego

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, czynnik jajowodowy
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["czynnik jajowodowy.(?!nie)"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={'czynnik jajowodowy.(?!nie)': 'czynnik_jajowodowy'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_nieprawidlowosci_macica_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca występowanie nieprawidłowości o obrębie macicy(polipy, zrosty, mieśniaki)

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, polipy, zrosty, mięśniaki
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["(pol.p)(?!.*nos)(?!.*strun)(?!.*nie)", "[ ]zrost", "miesniak"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={"(pol.p)(?!.*nos)(?!.*strun)(?!.*nie)":"polip",'[ ]zrost': 'zrosty'}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_budowa_macica_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca występowanie zaburzeń budowy jamy macicy (macica jednoroźnam dwuroźna, z przegrodą niejednorodna)

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, jednorożna, dwurożna,  przegroda, niejednorodna
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["jednorozn", "dwurozn", "(przegrod.)(?!.*nos.*)(?!.*kom.*)", "niejednorodn"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={"(przegrod.)(?!.*nos.*)(?!.*kom.*)":"przegrod"}, axis=1).groupby(['wizyta_id']).sum().reset_index()

def prepare_czynnik_meski_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["czynnik meski", "oligoastenosperm.*"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))
    
    dfs = pd.concat(dfs).drop_duplicates().rename(mapper={"czynnik meski":"czynnik_meski", "oligoastenosperm.*": "oligoastenospermia"}, axis=1).groupby(['wizyta_id']).sum().reset_index()

    dfs["sum"] = dfs[["czynnik_meski", "oligoastenospermia"]].sum(axis=1)
    
    dfs["czynnik_meski"] = np.where(dfs["sum"] >0, 1, dfs["czynnik_meski"])
    dfs.drop(["oligoastenospermia", "sum"], axis=1, inplace=True)
    
    return dfs

def prepare_idiopatyczna_dataset(sources=['wizyta_opis']) -> pd.DataFrame:
    """Funkcja zwracająca informacje o występowaniu nieplodnosci idiopatycznej u pacjentki

    Parameters
    ----------
    sources : list, optional
        źródła, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, nieplodnosc idiopatyczna
    """
    dfs = []
    for source in sources:
        dfs.append(get_data_from_wizyta_opis(source=source, return_cols=["^((?!malo.*).(?!skol.*).)*(idiopat.*)"], numeric=False, opis="slownik_badanie_fizykalne_p1_cat"))

    return pd.concat(dfs).drop_duplicates().rename(mapper={"^((?!malo.*).(?!skol.*).)*(idiopat.*)":"idiopat"}, axis=1).groupby(['wizyta_id']).sum().reset_index()


def prepare_kariotyp_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja zwracająca informacje o kariotypie (prawidłowy / nieprawidłowy)

    Parameters
    ----------
    sources : list, optional
        źródło wizyta_opis, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta_id, kariotyp
    """

    dfs = []
    for source in sources:
        dfs.append(
            get_data_from_wizyta_opis(
                source=source,
                return_cols=["kariotyp.*nieprawid.ow.", "kariotyp.*[ ]prawid.ow."],
                numeric=False,
                opis="slownik_badanie_fizykalne_p1_cat",
            )
        )
    dfs = (
        pd.concat(dfs)
        .drop_duplicates()
        .rename(
            mapper={
                "kariotyp.*nieprawid.ow.": "kariotyp_nieprawidlowy",
                "kariotyp.*[ ]prawid.ow.": "kariotyp_prawidlowy",
            },
            axis=1,
        )
        .groupby(["wizyta_id"])
        .sum()
        .reset_index()
    )
    dfs["kariotyp_prawidlowy"] = np.where(
        dfs.kariotyp_nieprawidlowy == 1, 0, dfs.kariotyp_prawidlowy
    )
    dfs["kariotyp_nieprawidłowy"] = np.where(
        dfs.kariotyp_nieprawidlowy == 1,
        1,
        np.where(dfs.kariotyp_prawidlowy == 1, 0, np.NaN),
    )
    dfs.drop(["kariotyp_prawidlowy", "kariotyp_nieprawidlowy"], axis=1, inplace=True)

    return dfs


def prepare_diabetes_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja zwracajaca informacje o występowaniu cukrzycy u Pacjenta

    Parameters
    ----------
    sources : list, optional
        źrodło danych wyniki_opis, by default ["wizyta_opis"]

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta_id, cukrzyca
    """
    dfs = []
    for source in sources:
        dfs.append(
            get_data_from_wizyta_opis(
                return_cols=["^((?!ma.*).(?!ojciec.*).(?!rodzi.*))*cukrzyc.(?! nie)"]
            )
        )
    return (
        pd.concat(dfs)
        .drop_duplicates()
        .rename(
            mapper={
                "^((?!ma.*).(?!ojciec.*).(?!rodzi.*))*cukrzyc.(?! nie)": "cukrzyca"
            },
            axis=1,
        )
        .groupby(["wizyta_id"])
        .sum()
        .reset_index()
    )

def prepare_characterictic_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja zwracająca tabelę, gdzie kolumny to wizyta id oraz informacje wyciągnięte ze żrodla wizyta_opis

    Parameters
    ----------
    sources : list, optional
        źródło, by default ["wizyta_opis"]

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, RIF, endometrioza, czynnik jajnikowy, niedoczynnosc tarczycy, rezerwa jajnikowa, PCOs, czynnik jajowodowy, nieprawidłowości maicyc, nieprawidłowości w budowie macicy, czynnik męski, niepłodność idiopatyczna
    """
    dfs = []
    for source in sources:
        dfs.append(
            get_data_from_wizyta_opis(
                return_cols=[
                    "[ ]rif[^ua]",
                    "endometrioz.(?![-nie| - nie|?])",
                    "czynnik jajnikowy.(?!nie)",
                    "nie.*tarczycy",
                    "pcos(?![ podejrz*])",
                    "czynnik jajowodowy.(?!nie)",
                    "(pol.p)(?!.*nos)(?!.*strun)(?!.*nie)",
                    "[ ]zrost",
                    "miesniak",
                    "jednorozn",
                    "dwurozn",
                    "(przegrod.)(?!.*nos.*)(?!.*kom.*)",
                    "niejednorodn",
                    "czynnik meski",
                    "^((?!malo.*).(?!skol.*).)*(idiopat.*)",
                    "kariotyp.*nieprawid.ow.",
                    "kariotyp.*[ ]prawid.ow.",
                    "torbiel.jajnik",
                    "^((?!ma.*).(?!ojciec.*).(?!rodzi.*))*cukrzyc.(?! nie)",
                    "olig.*sperm",
                    "brak plemnikow", 
                    "plemnikow nie stwierdzono", 
                    "teratozoosperm.*", 
                    "astenozoosperm.*", 
                    "azoosperm.*",
                    "endometria.*"
                ]
            )
        )
    dfs = (
        pd.concat(dfs)
        .drop_duplicates()
        .rename(
            mapper={
                "[ ]rif[^ua]": "rif",
                "endometrioz.(?![-nie| - nie|?])": "endometrioza",
                "czynnik jajnikowy.(?!nie)": "czynnik_jajnikowy",
                "nie.*tarczycy": "niedoczynnosc_tarczycy",
                "pcos(?![ podejrz*])": "policystyczne_jajniki",
                "czynnik jajowodowy.(?!nie)": "czynnik_jajowodowy",
                "(pol.p)(?!.*nos)(?!.*strun)(?!.*nie)": "polip",
                "[ ]zrost": "zrosty",
                "(przegrod.)(?!.*nos.*)(?!.*kom.*)": "przegrod",
                "czynnik meski": "czynnik_meski",
                "^((?!malo.*).(?!skol.*).)*(idiopat.*)": "idiopat",
                "kariotyp.*nieprawid.ow.": "kariotyp_nieprawidlowy",
                "kariotyp.*[ ]prawid.ow.": "kariotyp_prawidlowy",
                "torbiel.jajnik": "torbiel",
                "^((?!ma.*).(?!ojciec.*).(?!rodzi.*))*cukrzyc.(?! nie)":"cukrzyca",
                "olig.*sperm" : "oligospermia",
                "brak plemnikow" : "brak_plemnikow", 
                "plemnikow nie stwierdzono" : "plemnikow_nie_stwierdzono", 
                "teratozoosperm.*" : "teratozoospermia", 
                "astenozoosperm.*" : "astenozoospermia", 
                "azoosperm.*": "azoospermia",
                "endometria.*":"torbiele_endometrialne"
            },
            axis=1,
        )
        .groupby(["wizyta_id"])
        .sum()
        .reset_index()
    )
    dfs["kariotyp_prawidlowy"] = np.where(
        dfs.kariotyp_nieprawidlowy == 1, 0, dfs.kariotyp_prawidlowy
    )
    dfs["kariotyp_nieprawidłowy"] = np.where(
        dfs.kariotyp_nieprawidlowy == 1,
        1,
        np.where(dfs.kariotyp_prawidlowy == 1, 0, np.NaN),
    )
    dfs.drop(["kariotyp_prawidlowy", "kariotyp_nieprawidlowy"], axis=1, inplace=True)

    return dfs

def prepare_numeric_dataset(sources=["wizyta_opis"]) -> pd.DataFrame:
    """Funkcja zwracająca tabelę, gdzie kolumny to wizyta id oraz numeryczne informacje wyciągnięte ze źródła wizyta_opis
    Parameters
    ----------
    sources : list, optional
        źródło, by default ['wizyta_opis']

    Returns
    -------
    pd.DataFrame
        Tabela: wizyta id, menarche, BMI, waga, wzrost,
    """
    dfs = []
    for source in sources:
        dfs.append(
            get_data_from_wizyta_opis(
                source=source,
                numeric=True,
                return_cols=["menarche", "wzrost", "waga", "AMH"],
                opis="slownik_badanie_fizykalne_p1_num",
            )
        )

    dfs = pd.concat(dfs).drop_duplicates()
    dfs["wzrost"] = np.where(
        dfs["wzrost"] > 250,
        np.NaN,
        np.where(dfs["wzrost"] < 100, np.NaN, dfs["wzrost"] / 100),
    )
    dfs["waga"] = np.where(
        dfs["waga"] > 250, np.NaN, np.where(dfs["waga"] < 35, np.NaN, dfs["waga"])
    )
    dfs["BMI"] = np.round(
        (
            np.where(
                (dfs["wzrost"].notna() & dfs["wzrost"].notna()),
                dfs["waga"] / dfs["wzrost"] ** 2,
                np.NaN,
            )
        ),
        2,
    )
    dfs["AMH"] = np.where(dfs["AMH"] > 20, np.NaN, dfs["AMH"])
    dfs["obnizona_rezerwa"] = np.where(
        dfs["AMH"] < 1, 1, np.where(dfs["AMH"].isna(), np.NaN, 0)
    )
    return dfs

def prepare_all_wizyta_opis() -> pd.DataFrame:
    """Funkcja łącząca tabele ze źródła wizyta_opis: wyodrębnione słowa, dane numeryczne oraz binanre(dla papierosów)

    Returns
    -------
    pd.DataFrame
        Funkcja zwraca tabelę binarną bądź numeryczną z informacjami wyciągniętymi z kompendium
    """
    all_dfs = [
        prepare_characterictic_dataset(),
        prepare_numeric_dataset(),
        prepare_papierosy_dataset(),
    ]

    dfs = (
        pd.concat(all_dfs).drop_duplicates().groupby(["wizyta_id"]).max().reset_index()
    )

    return dfs