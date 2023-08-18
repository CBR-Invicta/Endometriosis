from typing import List, Dict, Any, Optional, Tuple, Iterable, Any, Generator
from dataclasses import dataclass
from collections import OrderedDict
import pandas as pd
import numpy as np
import lightgbm as lgb
from math import sqrt
from dotenv import load_dotenv

import LGBM_metrics
from LGBM_metrics import RMSE, ErrorsInfo, errors_info_keys_generator
from prepare_data_series import DataSerie


@dataclass
class FoldInfo:
    target_col: str
    train_cols: List[str]
    train_df: pd.DataFrame
    test_df: pd.DataFrame
    lgb_models: Dict[str, lgb.basic.Booster]
    test_errors_info: ErrorsInfo
    train_errors_info: ErrorsInfo


@dataclass
class TrainInfo:
    input_df: pd.DataFrame
    target_col: str
    fold_infos: List[FoldInfo]
    avg_test_errors_info: ErrorsInfo
    avg_train_errors_info: ErrorsInfo

    def plot_learning_progress(self, plot_metrics: List[str], plot_sufixes: List[str]):

        for model_suffix in plot_sufixes:
            for metric in plot_metrics:
                for fold, fold_info in enumerate(self.fold_infos):
                    evals_result = fold_info.evals_results[model_suffix]

                    title = f"model: {model_suffix}, metric: {metric}, fold: {fold}"
                    lgb.plot_metric(evals_result, metric=metric, title=title)


@dataclass
class TrainResults:
    train_cols: List[str]
    train_infos: Dict[str, TrainInfo]

    def print_errors(
        self,
        base_results_list: Optional[List["TrainResults"]] = None,
        print_suffixes: Optional[List[str]] = None,
        print_metrics: Optional[List[str]] = None,
        print_folds: bool = False,
        print_avg: bool = False,
        print_train: bool = False,
    ):

        # Results for folds
        if print_folds:
            for error_metric in errors_info_keys_generator():
                if print_suffixes is not None:
                    continue
                if print_metrics is not None and error_metric not in print_metrics:
                    continue
                for data_serie_name, train_info in self.train_infos.items():
                    if base_results_list is not None:
                        base_test_errors_infos = [
                            base_results.train_infos[
                                data_serie_name
                            ].avg_test_errors_info
                            for base_results in base_results_list
                        ]
                        base_train_errors_infos = [
                            base_results.train_infos[
                                data_serie_name
                            ].avg_train_errors_info
                            for base_results in base_results_list
                        ]
                    else:
                        base_test_errors_infos = None
                        base_train_errors_infos = None
                    for fold, fold_info in enumerate(train_info.fold_infos):
                        fold_info.test_errors_info.print_error(
                            data_serie_name,
                            f" test_fold_{fold}",
                            error_metric,
                            base_test_errors_infos,
                        )
                    if print_train:
                        for fold, fold_info in enumerate(train_info.fold_infos):
                            fold_info.train_errors_info.print_error(
                                data_serie_name,
                                f"train_fold_{fold}",
                                error_metric,
                                base_train_errors_infos,
                            )
                    print("-")

        # Results for avg
        if print_avg:
            for data_serie_name, train_info in self.train_infos.items():
                prev_model_suffix = None
                for error_metric in errors_info_keys_generator():
                    if (
                        print_suffixes is not None
                    ):
                        continue
                    if print_metrics is not None and error_metric not in print_metrics:
                        continue
                    if base_results_list is not None:
                        base_test_errors_infos = [
                            base_results.train_infos[
                                data_serie_name
                            ].avg_test_errors_info
                            for base_results in base_results_list
                        ]
                        base_train_errors_infos = [
                            base_results.train_infos[
                                data_serie_name
                            ].avg_train_errors_info
                            for base_results in base_results_list
                        ]
                    else:
                        base_test_errors_infos = None
                        base_train_errors_infos = None
                    train_info.avg_test_errors_info.print_error(
                        data_serie_name,
                        f" test_fold_avg",
                        error_metric,
                        base_test_errors_infos,
                    )
                    if print_train:
                        train_info.avg_train_errors_info.print_error(
                            data_serie_name,
                            f"train_fold_avg",
                            error_metric,
                            base_train_errors_infos,
                        )
                print("-----------------------------")

    def get_merged_test_dfs_from_folds(self, data_serie_name: str) -> pd.DataFrame:
        return pd.concat(
            [
                fold_info.test_df
                for fold_info in self.train_infos[data_serie_name].fold_infos
            ]
        )

    def print_rmse_for_filter(self, filter_tuples: List[Tuple[str, str]]):

        for data_serie_name, train_info in self.train_infos.items():

            for filter_tuple in filter_tuples:
                df = pd.concat(
                    [fold_info.test_df for fold_info in train_info.fold_infos]
                )
                df = df[df[filter_tuple[0]] == filter_tuple[1]]
                rmse_mid = RMSE(df["prediction_l2"], df[train_info.target_col])

                print(
                    f'{data_serie_name.ljust(25, " ")}: '
                    f'{filter_tuple[1].ljust(35, " ")}: '
                    f'count: {str(len(df)).ljust(5, " ")}    : '
                    f'RMSE: {"%.2f"%rmse_mid}'
                )
            print("-")


LGB_PARAMS = {
    "objective": "multiclass",
    "num_class": 3,
    "metric": "multi_logloss",
    "random_state": 42,
}


def train_fold(
    LGB_PARAMS: Dict[str, Any],
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    train_cols: List[str],
    target_col: str,
) -> FoldInfo:

    train_dataset = lgb.Dataset(
        train_df[train_cols],
        label=train_df[target_col],
    )
    test_dataset = lgb.Dataset(
        test_df[train_cols],
        label=test_df[target_col],
        reference=train_dataset,
    )
    if len(test_df) == 0:
        valid_datasets = [train_dataset]
    else:
        valid_datasets = [train_dataset, test_dataset]

    model = lgb.LGBMClassifier(**LGB_PARAMS)

    model.fit(
        X=train_df[train_cols],
        y=train_df[target_col],
        eval_set=[(test_df[train_cols], test_df[target_col])],
        eval_metric='multi_logloss',
        verbose=False,
    )
    if len(test_df) != 0:
        test_df["prediction"] = model.predict(
            test_df[train_cols])
    train_df["prediction"] = model.predict(train_df[train_cols])

    fold_info = FoldInfo(
        target_col=target_col,
        train_cols=train_cols,
        train_df=train_df,
        test_df=test_df,
        lgb_models=model,
        test_errors_info=ErrorsInfo().calculate_errors(
            test_df, target_col
        ),
        train_errors_info=ErrorsInfo().calculate_errors(
            train_df, target_col
        ),

    )
    return fold_info


def train_data_serie(
    LGB_PARAMS_BASE: Dict[str, Any],
    data_serie: DataSerie,
    train_cols: List[str]
) -> TrainInfo:

    for train_col in train_cols:
        if train_col not in data_serie.columns:
            print(f"WARNING: Column {train_col} not in data serie")
            return TrainInfo(
                input_df=pd.DataFrame(),
                target_col=data_serie.target_col,
                fold_infos=[],
                avg_test_errors_info=ErrorsInfo(
                    count=0,
                    target_avg=0,
                    errors_info={},
                ),
                avg_train_errors_info=ErrorsInfo(
                    count=0,
                    target_avg=0,
                    errors_info={},
                ),
            )

    fold_infos = []
    for _fold, split in enumerate(data_serie.splits):
        fold_info = train_fold(
            LGB_PARAMS_BASE,
            split.train_df.copy(),
            split.test_df.copy(),
            train_cols,
            data_serie.target_col
        )
        fold_infos.append(fold_info)

    train_info = TrainInfo(
        input_df=data_serie.input_df,
        target_col=data_serie.target_col,
        fold_infos=fold_infos,
        avg_test_errors_info=ErrorsInfo().calculate_avg_test_errors(
            [fold_info.test_df for fold_info in fold_infos],
            data_serie.target_col
        ),
        avg_train_errors_info=ErrorsInfo().calculate_avg_train_errors(
            [fold_info.train_errors_info for fold_info in fold_infos]
        ),
    )

    return train_info


def train_data_series(
    LGB_PARAMS_BASE: Dict[str, Any],
    data_series: Dict[str, DataSerie],
    train_cols: List[str],
) -> TrainResults:

    train_infos = {}
    for data_serie_name in data_series.keys():
        train_infos[data_serie_name] = train_data_serie(
            LGB_PARAMS_BASE,
            data_series[data_serie_name],
            train_cols,
        )

    return TrainResults(train_cols=train_cols, train_infos=train_infos)
