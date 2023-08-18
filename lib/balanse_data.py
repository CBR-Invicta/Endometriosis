from regex import F
from sqlalchemy import true
import pandas as pd
from typing import Dict, Any, List
import numpy as np
from imblearn.over_sampling import SMOTE, ADASYN


settings = {"strategy": "auto", "sampler": SMOTE}
# strategy in ['minority', 'not minority', 'not majority', 'all', 'auto', 'custom']
# sampler in [SMOTE, ADASYN]


class balanse_data:
    """Klasa do balansowania zbiorów danych przed modelowaniem. Obsługuje problemy klasyfikacji binarnej, multiclass i multilabel classification.

    Attributes:
    ----------
        data:pd DataFrame
            Wejściowy zbiór danych
        y_columns: List[str]
            Lista kolumn zależnych
        balansed_data: pd.DataFrame
            Zbiór danych po oversamplingu
        settings: Dict[str, Any]
            Słownik ustawień algorytmów:
                strategy - sposób oversamplingu, wartość z listy ['minority', 'not minority', 'not majority', 'all', 'auto', 'custom']
                sampler - model oversamplujący, jeden z SMOTE, ADASYN

    Methods:
    ----------
        balanse(self):
            Metodą wykonującą oversampling na wejściowym zbiorze danych.

    """

    def __init__(
        self,
        data: pd.DataFrame = None,
        y_columns: List[str] = None,
        settings: Dict[str, Any] = settings,
    ):
        self.data: pd.DataFrame = data
        self.y_columns: List[str] = y_columns
        self.balansed_data: pd.DataFrame = None
        self.settings: Dict[str, Any] = settings

    def balanse(self):
        """balanse_data jest metodą wykonującą oversampling na wejściowym zbiorze danych.

        Na podstawie atrybutów klasy odpowiedni model z odpowiednią strategią dokonuje oversamplingu na zbiorze danych.
        Zmienne wejściowe i wyjściowe są mergowane i zwracane jako atrybut self.balanced data
        """
        model = self.settings["sampler"]
        strategy = self.settings["strategy"]
        multiple_y_cols = len(self.y_columns) > 1
        if multiple_y_cols is True:
            self.data["label"] = (
                self.data[self.y_columns]
                .astype(str)
                .apply(lambda x: "_".join(x), axis=1)
            )
        else:
            self.data["label"] = self.data[self.y_columns].copy()
        if strategy == "custom":
            weight = (self.data["label"].value_counts()).apply(
                lambda x: 1 / (np.sqrt(x))
            )
            weight = weight * (1 / min(weight))
            strategy = dict(round(self.data["label"].value_counts() * weight))

        oversample = model(sampling_strategy=strategy)
        data_X = self.data.drop(columns=self.y_columns + ["label"])
        data_Y = self.data.label
        smote_sampler = oversample.fit(data_X, data_Y)
        data_X, data_Y = smote_sampler.fit_resample(data_X, data_Y)
        if multiple_y_cols is True:
            data_Y = (
                data_Y.str.split("_")
                .apply(pd.Series)
                .astype(float)
                .rename(columns=dict(zip(range(len(self.y_columns)), self.y_columns)))
            )
        else:
            data_Y = data_Y.rename(self.y_columns[0])
        self.balansed_data = data_X.merge(data_Y, left_index=True, right_index=True)
