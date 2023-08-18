import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Iterable, Any, Generator
from sklearn.model_selection import GroupKFold, ShuffleSplit, KFold, StratifiedKFold, train_test_split
from utils import check_objective


@dataclass
class TrainTestFoldSplit:
    train_df: pd.DataFrame
    test_df: pd.DataFrame


@dataclass
class DataSerie:
    input_df: pd.DataFrame
    target_col: str
    splits: List[TrainTestFoldSplit]
    columns: List[str]


def split_train_test(input_df: pd.DataFrame, target_col: str, test_size: float = 0.2, dropna=False):
    if test_size == 0:
        input_df.dropna(inplace=True, subset=target_col)
        if dropna:
            return input_df.dropna().drop(columns=target_col), input_df.dropna()[target_col], None, None
        return input_df.drop(columns=target_col), input_df[target_col], None, None
    objective = check_objective(input_df[target_col])
    if objective == 'classification':
        X_train, X_test, y_train, y_test = train_test_split(input_df.drop(
            columns=target_col), input_df[target_col], test_size=test_size, random_state=42, stratify=input_df[target_col])
    else:
        X_train, X_test, y_train, y_test = train_test_split(input_df.drop(
            columns=target_col), input_df[target_col], test_size=test_size, random_state=42)
    return X_train, X_test, y_train, y_test


def split_fold_train_test(input_df: pd.DataFrame, target_col: str, n_folds: int = 1, random_state: int=42) -> List[TrainTestFoldSplit]:

    objective = check_objective(input_df[target_col])
    if objective == 'classification':
        split_generator = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=random_state).split(
            input_df, input_df[target_col]
        )
    else:
        split_generator = KFold(n_splits=n_folds, shuffle=True, random_state=random_state).split(
            input_df, input_df[target_col]
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
            TrainTestFoldSplit(
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


def prepare_data_serie(input_df: pd.DataFrame, target_col: str, n_folds: int = 1) -> DataSerie:

    input_df = input_df.copy()

    print("====================================")
    print(f'Original records: {len(input_df)}')

    return DataSerie(
        input_df=input_df,
        target_col=target_col,
        splits=split_fold_train_test(input_df, target_col, n_folds),
        columns=list(input_df.columns),
    )


def prepare_data_series(
    input_df: pd.DataFrame, target_col: str, n_folds: int = 1
) -> Tuple[Dict[str, DataSerie]]:

    DATA_SERIES = {}

    DATA_SERIES[target_col] = prepare_data_serie(
        input_df, target_col, n_folds
    )

    return DATA_SERIES  # , {**DATA_SERIES}
