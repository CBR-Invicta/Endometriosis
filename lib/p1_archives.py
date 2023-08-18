
def process_question_487(data: pd.DataFrame, question_id: int = 487) -> pd.DataFrame:
    """
    Przygotowanie pytania o aktywność sportową.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 487

    Returns
    -------
    pd.DataFrame
        Kolumny zawierające informacje o aktywnosci sportowa
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
            for y in data.loc[data.id_question == question_id]
            .odpowiedzi.str.replace('"', "", regex=False)
            .str.replace("[", "", regex=False)
            .str.split(",")
        ],
        name="Aktywnosc sportowa",
    )
    response.replace("null", "NO", inplace=True)
    response = pd.get_dummies(response, drop_first=False)
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response

def process_question_489(data: pd.DataFrame, question_id: int = 489) -> pd.Series:
    """
    Przygotowanie pytania o muzykalnosc.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 489

    Returns
    -------
    pd.Series
        Zmienna zawierająca informację o muzykalności (1-tak, 0-nie)
    """
    nazwa_zmiennej = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id].pytanie_pl.iloc[0].replace(" ", "_")
    )
    response = (
        data.loc[data.id_question == question_id]
        .odpowiedzi.apply(lambda x: re.match(".*YES.*", x) is not None)
        .astype(int)
        .rename(nazwa_zmiennej)
    )
    response = pd.DataFrame(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_154(data: pd.DataFrame, question_id: int = 154) -> pd.DataFrame:
    """
    Przygotowanie pytania o zazywanie narkotykow.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 154

    Returns
    -------
    pd.DataFrame
        Tabela zawierająca informacje binarne o tym, czy dana osoba kiedykolwiek spożywała dany narkotyk.
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
    for column_name in response:
        response[column_name] = response[column_name].apply(
            lambda x: int(sum(pd.notna(x)) > 0)
        )
    response = response[response.columns[(response.sum(axis=0) > 15)]]
    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    return response


def process_question_503(data: pd.DataFrame, question_id: int = 503) -> pd.DataFrame:
    """
    Przygotowanie pytania o zazywanie narkotykow.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 503

    Returns
    -------
    pd.DataFrame
        Tabela zawierająca informacje binarne o tym, czy dana osoba kiedykolwiek spożywała dany narkotyk.
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
    for column_name in response:
        response[column_name] = response[column_name].apply(
            lambda x: int(sum(pd.notna(x)) > 0)
        )
    response = response[response.columns[(response.sum(axis=0) > 15)]]
    response = response.add_prefix(prefix=prefix)
    return response


def process_question_215(data: pd.DataFrame, question_id: int = 215) -> pd.DataFrame:
    """Przygotowanie pytania o wykonane badania podczas dotychczasowej diagnostyki.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 215

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi o wykonaniu badania podczas dotychczasowej diagnostyki
    """
    response = data.loc[data.id_question == question_id].odpowiedzi
    response1 = (
        response.loc[(response != "NO") & (response != "YES")]
        .map(json.loads)
        .apply(pd.Series)
    )
    response2 = response.loc[(response == "NO") | (response == "YES")].apply(pd.Series)
    response2 = pd.get_dummies(response2)
    diagnostic = []
    Diagnostic = {}

    for column_name in response1:
        diagnostic = response1[column_name].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        Diagnostic[column_name] = diagnostic

    df_diagnostic = pd.DataFrame.from_dict(Diagnostic)

    for column_name in df_diagnostic:
        df_diagnostic[column_name] = (
            df_diagnostic[column_name]
            .str.replace(",", ".")
            .map(dict(TRUE=1, FALSE=0, NONE=np.NaN))
        )

    result = pd.merge(
        df_diagnostic, response2, how="outer", left_index=True, right_index=True
    )
    result = result.rename(columns={"0_YES": "YES", "0_NO": "NO"})
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    result = result.add_prefix(prefix=prefix)
    result.set_index(data.loc[data.id_question == question_id].wizyta_id, inplace=True)

    return result

def process_question_236(data: pd.DataFrame, question_id: int = 236) -> pd.DataFrame:
    """Przygotowanie pytania o wykonane badania podczas dotychczasowej diagnostyki.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 236

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi o wykonaniu badania podczas dotychczasowej diagnostyki
    """
    response = data.loc[data.id_question == question_id].odpowiedzi
    response1 = (
        response.loc[(response != "NO") & (response != "YES")]
        .map(json.loads)
        .apply(pd.Series)
    )
    response2 = response.loc[(response == "NO") | (response == "YES")].apply(pd.Series)
    response2 = pd.get_dummies(response2)
    diagnostic = []
    Diagnostic = {}

    for column_name in response1:
        diagnostic = response1[column_name].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        Diagnostic[column_name] = diagnostic

    df_diagnostic = pd.DataFrame.from_dict(Diagnostic)

    for column_name in df_diagnostic:
        df_diagnostic[column_name] = (
            df_diagnostic[column_name]
            .str.replace(",", ".")
            .map(dict(TRUE=1, FALSE=0, NONE=np.NaN))
        )

    result = pd.merge(
        df_diagnostic, response2, how="outer", left_index=True, right_index=True
    )
    result = result.rename(columns={"0_YES": "YES", "0_NO": "NO"})
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    result = result.add_prefix(prefix=prefix)
    result.set_index(data.loc[data.id_question == question_id].wizyta_id, inplace=True)

    return result

def process_question_106(data: pd.DataFrame, question_id: int = 106) -> pd.DataFrame:
    """Przygotowanie pytania o wykonane badania podczas dotychczasowej diagnostyki.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 106

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi o wykonaniu badania podczas dotychczasowej diagnostyki
    """
    response = data.loc[data.id_question == question_id].odpowiedzi
    response1 = (
        response.loc[(response != "NO") & (response != "YES")]
        .map(json.loads)
        .apply(pd.Series)
    )
    response2 = response.loc[(response == "NO") | (response == "YES")].apply(pd.Series)
    response2 = pd.get_dummies(response2)
    diagnostic = []
    Diagnostic = {}

    for column_name in response1:
        diagnostic = response1[column_name].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        Diagnostic[column_name] = diagnostic

    df_diagnostic = pd.DataFrame.from_dict(Diagnostic)

    for column_name in df_diagnostic:
        df_diagnostic[column_name] = (
            df_diagnostic[column_name]
            .str.replace(",", ".")
            .map(dict(TRUE=1, FALSE=0, NONE=np.NaN))
        )

    result = pd.merge(
        df_diagnostic, response2, how="outer", left_index=True, right_index=True
    )
    result = result.rename(columns={"0_YES": "YES", "0_NO": "NO"})
    prefix = (
        str(question_id)
        + "_"
        + data.loc[data.id_question == question_id]
        .pytanie_pl.iloc[0]
        .replace(" ", "_")
        .replace(":", "")
        + ":"
    )

    result = result.add_prefix(prefix=prefix)
    result.set_index(data.loc[data.id_question == question_id].wizyta_id, inplace=True)

    return result


def process_question_366(data: pd.DataFrame, question_id: int = 366) -> pd.DataFrame:
    """Przygotowanie pytania: czy oddała Pani komórki w praogramie dawstwa (podział na liczbę procedur oddania komórek).

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 139

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami numerycznymi jako liczba programów dawstwa, w których brała udział
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
        data.loc[data.id_question == 366]
        .odpowiedzi.str.replace(".*ONE.*", "ONE", regex=True)
        .str.replace(".*TWO.*", "TWO", regex=True)
        .str.replace(".*THREE.*", "THREE", regex=True)
        .str.replace(".*FOUR.*", "FOUR", regex=True)
        .str.replace(".*FIVE.*", "FIVE", regex=True)
        .map(dict(NO=0, ONE=1, TWO=2, THREE=3, FOUR=4, FIVE=5))
    )
    response = pd.DataFrame(response)

    response = response.add_prefix(prefix=prefix)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )

    return response

def process_question_80(data: pd.DataFrame, question_id: int = 80) -> pd.DataFrame:
    """Przygotowanie pytania o kolor oczu. Ujednolicenie formatu odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 80

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla koloru oczu
    """

    response = process_single_select_questions(data, 80).add_suffix("_COLOR")

    return response

def process_question_82(data: pd.DataFrame, question_id: int = 82) -> pd.DataFrame:
    """Przygotowanie pytania o naturalny kolor włosów. Ujednolicenie formatu odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 82

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla każdego naturalnego koloru włosów
    """

    response = process_single_select_questions(data, 82).add_suffix("_COLOR")

    return response


def process_question_84(data: pd.DataFrame, question_id: int = 84) -> pd.DataFrame:
    """Przygotowanie pytania o strukturę włosów. Ujednolicenie formatu odpowiedzi.

    Parameters
    ----------
    data : pd.DataFrame
        Zespół danych wejściowych
    question_id : int, optional
        id pytania, by default 84

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla struktury włosów
    """

    response = (process_single_select_questions(data, 84)).add_suffix("_SHAPE")

    response.columns = response.columns.str.replace("Struktura_włosów", "Skręt_włosów")

    return response


def process_question_609(data: pd.DataFrame, question_id: int = 609) -> pd.DataFrame:
    """Przygotowanie pytania o przynależności etnicznej.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    question_id : int, optional
        id pytania, by default 609

    Returns
    -------
    pd.DataFrame
        Tabela z wartościami binarnymi dla każdej grupy etnicznej
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
    response = data.loc[data.id_question == question_id].odpowiedzi.str.split(
        ";", expand=True
    )
    response[1] = response[1].str.extract(r"\"([A-Z]*_ETHNICITY)\"", expand=True)
    response = pd.get_dummies(response)
    response.set_index(
        data.loc[data.id_question == question_id].wizyta_id, inplace=True
    )
    response.columns = response.columns.str.replace("0", "PRIMARY").str.replace(
        "1", "SECONDARY"
    )

    response = response.add_prefix(prefix=prefix)

    return response

    