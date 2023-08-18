from utils import DataSerieEDA
import os
import sys
import pandas as pd
import numpy as np
from typing import List
from dotenv import load_dotenv
import pickle
load_dotenv()
sys.path.insert(0, os.getenv('lib_path'))


# rules levels

# funkcja: del_repeated_value_columns

# funkcja: del_low_freq_col
# funkcja: del_missing_over_level


def del_single_value_columns(data: pd.DataFrame, ignore_cols: List[str] = []) -> pd.DataFrame:
    """Usunięcie kolumn ze zbioru danych, które posiadają pojedynczą wartość

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych po przeprocesowaniu 

    Returns
    -------
    pd.DataFrame
        Tabela z usuniętymi kolumnami, posiadającymi pojedynczą wartość
    """
    to_del = data.columns[data.nunique() == 1].to_list()
    to_del = list(set(to_del) - set(ignore_cols))
    to_del.sort()
    data.drop(columns=to_del, inplace=True)

    return data, to_del


def del_repeated_value_columns(data: pd.DataFrame, ignore_cols: List[str] = [], repeated_values_level: float = 90) -> pd.DataFrame:
    """"Usunięcie kolumn z tą samą wartością na poziomie repeated_values_level

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych po przeprocesowaniu

    Returns
    -------
    pd.DataFrame
        Zbiór danych z usuniętymi kolumnami, których występowanie wartości jest na poziomie repeated_values_level
    """
    num_rows = len(data.index)
    to_del = []

    for col in data.columns:
        counts = data[col].value_counts(dropna=False)
        top_pct = (counts/num_rows).iloc[0]

        if top_pct > (repeated_values_level/100):
            to_del.append(col)
    to_del = list(set(to_del) - set(ignore_cols))

    data.drop(to_del, axis=1, inplace=True)
    to_del.sort()
    return data, to_del


def del_low_freq_col(data: pd.DataFrame, ignore_cols: List[str] = [], frequency_level: float = 10) -> pd.DataFrame:
    """Przygotowanie danych do modelowania: Usunięcie kolumn o niskim procencie występowania wartości 1 (dla pytań binarnych). 
    Poziom występowania wartości odpowiada zmiennej frequency_level.

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych

    Returns
    -------
    pd.DataFrame
        Tabela z usuniętymi kolumnami o niskiej liczbie występowania wartości.
    """
    num_rows = len(data.index)
    to_del = []
    for column_name in data:
        counts = data[column_name].value_counts()
        if len(counts) == 2:
            unique_freq_list = dict(counts)
            for key in unique_freq_list:
                if key == 1:
                    if (unique_freq_list[key]/num_rows)*100 <= frequency_level:
                        to_del.append(column_name)
    to_del = list(set(to_del) - set(ignore_cols))
    data.drop(to_del, axis=1, inplace=True)
    to_del.sort()

    return data, to_del


def del_missing_over_level(data: pd.DataFrame, ignore_cols: List[str] = [], missing_level: float = 95) -> pd.DataFrame:
    """Usunięcie kolumn z ponad missing_level wartosciami NaN

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych przeprocesowanych

    Returns
    -------
    pd.DataFrame
        Zbiór danych z usuniętymi kolumnami
    """
    start_cols = data.columns.tolist()
    min_count = int(((100-missing_level)/100)*data.shape[0] + 1)
    to_del = list(set(start_cols) -
                  set(data.dropna(axis=1, thresh=min_count).columns.tolist()))
    to_del = list(set(to_del) - set(ignore_cols))
    data.drop(to_del, axis=1, inplace=True)
    to_del.sort()
    #print(f'Dropping columns with {missing_level} level of NaN')
    #print('Liczba kolumn i wierszy: ', data.shape)

    return data, to_del


class Rules:
    """
    Klasa przygotowująca dane usuwając mało znaczące dla modelowania kolumny. 

    ...

    Attributes
    ----------
    processed_data: pd.DataFrame
        Zawiera przeprocesowane wartości, gdzie 1 wiersz odpowiada 1 ankiecie - tabela jest wypełniana przez metodę `process_questions`

    Methods
    ----------
    process_rules(self) - usuwa kolumny o małym znaczeniu dla modelowania przy użyciu funkcji: del_single_value_columns, del_repeated_value_columns, del_low_freq_col, del_missing_over_level


    """

    def __init__(self, data: DataSerieEDA):
        """__init__

        Parameters
        ----------
        processed_data : pd.DataFramae
            Zbiór danych przeprocesowanych
        """
        self.data_serie: DataSerieEDA = data
        self.processed_data: pd.DataFrame = data.input_df[data.columns]
        self.filtered_data: DataSerieEDA = None
        self.removed_cols: dict = {}

    def process_rules(
            self,
            repeated_values_level: float = 100,  # 95,
            frequency_level: float = 2,
            missing_level: float = 95) -> pd.DataFrame:
        """process_rules przeprocesowuje zbiór danych wejściowych wedlug ustalonych zasad (usuwa niepotrzebne rekordy)

        Returns
        -------
        pd.DataFrame
            Tabela z zastosowanymi zasadami
        """
        ignore_cols = self.data_serie.y_columns
        print(
            f"Number of columns prior to processing = {self.processed_data.shape[1]}")
        print(f"Processing delete single value columns...")
        self.processed_data, self.removed_cols['del_single_value_columns'] = del_single_value_columns(
            self.processed_data, ignore_cols)
        print(
            f"Done. Current number of columns = {self.processed_data.shape[1]}")

        print(
            f"Processing delete repeated value columns with level {repeated_values_level}%...")
        self.processed_data, self.removed_cols['del_repeated_value_columns'] = del_repeated_value_columns(
            self.processed_data, ignore_cols, repeated_values_level)
        print(
            f"Done. Current number of columns = {self.processed_data.shape[1]}")

        print(
            f"Processing delete low value 1 frequency for binary columns with level below {frequency_level}%...")
        self.processed_data, self.removed_cols['del_low_freq_col'] = del_low_freq_col(
            self.processed_data, ignore_cols, frequency_level)
        print(
            f"Done. Current number of columns = {self.processed_data.shape[1]}")

        print(f"Processing delete missing over level {missing_level}%...")
        self.processed_data, self.removed_cols['del_missing_over_level'] = del_missing_over_level(
            self.processed_data, ignore_cols, missing_level)
        print(
            f"Done. Current number of columns = {self.processed_data.shape[1]}")

        print(f"Rules processing completed")
        print(f"Final number of columns = {self.processed_data.shape[1]}")
        self.filtered_data = DataSerieEDA(
            input_df=self.processed_data,
            y_columns=self.data_serie.y_columns,
            columns=self.processed_data.columns)

    def save_results(self):
        with open('premodelling_rules_results.pickle', 'wb') as handle:
            pickle.dump(vars(self), handle, protocol=pickle.HIGHEST_PROTOCOL)
