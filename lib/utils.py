import ipywidgets as widgets
import pandas as pd
from dataclasses import dataclass
from typing import List
from sklearn.model_selection import GroupKFold

from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
import numpy as np


def filter_data(df: pd.DataFrame, filter_serie: pd.Series) -> pd.DataFrame:

    filtered_df = df[filter_serie]
    # print(f'Removed records:  {len(df) - len(filtered_df)}')

    return filtered_df


class TabbedPlot:
    """IPy Tab Widget wrapper for plots

    In some cases plt.show() is neccessary  to function properly.

    Example:
        tabbed = TabbedPlot()
        for _ in range(5):
            with tabbed.add_plot():
                plt.plot(np.random.uniform(size=100))
                plt.show()
        tabbed.display()
    """

    def __init__(self) -> None:
        self._outputs = []
        self._titles = []
        self._tab_widget = widgets.Tab(children=self._outputs)

    def display(self):
        self._tab_widget = widgets.Tab(children=self._outputs)
        for idx, title in enumerate(self._titles):
            self._tab_widget.set_title(idx, title)
        display(self._tab_widget)

    def add_plot(self, tab_title: str = None):

        if tab_title is None:
            tab_title = f"Tab {len(self._outputs)}"
        self._titles.append(tab_title)

        self._outputs.append(widgets.Output())
        return self._outputs[-1]


@dataclass
class TrainTestSplit:
    train_df: pd.DataFrame
    test_df: pd.DataFrame


def split_train_test(input_df: pd.DataFrame, n_folds: int = 5) -> List[TrainTestSplit]:

    split_generator = GroupKFold(n_splits=n_folds).split(
        input_df, groups=input_df.index
    )

    test_users_cum = set()
    sum_len_test_df = 0
    splits = []
    for fold in range(0, n_folds):
        train_positions, test_positions = next(split_generator)

        train_df = input_df.iloc[train_positions].copy()
        test_df = input_df.iloc[test_positions].copy()
        sum_len_test_df += len(test_df)

        train_users = set(train_df.index.unique())
        test_users = set(test_df.index.unique())
        assert len(train_df) + len(test_df) == len(
            input_df
        ), "Invalid split - not all records consumed by test/train"
        assert (
            test_users & train_users
        ) == set(), "Invalid split - intersection test_users/train_users not empty"
        assert (
            test_users & test_users_cum
        ) == set(), "Invalid split - intersection test_users/test_users_cum not empty"
        test_users_cum.update(test_users)

        splits.append(
            TrainTestSplit(
                train_df=train_df,
                test_df=test_df,
            )
        )

    assert len(test_users_cum) == len(
        input_df.index.unique()
    ), "Invalid split - not all users consumed by test"
    assert sum_len_test_df == len(
        input_df
    ), "Invalid split - not all records consumed by test"

    return splits


def impute_data(data: pd.DataFrame, iterative: bool = False) -> pd.DataFrame:
    """impute_data wypełnia braki danych medianą zmiennej

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór wejściowy z brakami danych
    iterative : bool
        Czy użyć techniki imputacji wielokrotnej? Odradzane dla dużych zbiorów danych przez długi czas obliczeniowy, by default False

    Returns
    -------
    pd.DataFrame
        Zbiór danych bez braków danych
    """
    if iterative:
        imp = IterativeImputer(max_iter=10, random_state=0)
    else:
        imp = SimpleImputer(strategy="median")
    result = imp.fit_transform(data)
    # result = imp.transform(data)
    result = pd.DataFrame(result, columns=data.columns, index=data.index)
    return result


@dataclass
class DataSerieEDA:
    input_df: pd.DataFrame
    y_columns: List[str]
    columns: List[str]


@dataclass
class DataSerieModelling:
    input_df: pd.DataFrame
    y_columns: List[str]
    splits: List[TrainTestSplit]
    columns: List[str]


def check_objective(y):
    if (np.unique(y).__len__() == 2) | pd.api.types.is_string_dtype(y):
        objective = "classification"
    else:
        objective = "regression"
    return objective
